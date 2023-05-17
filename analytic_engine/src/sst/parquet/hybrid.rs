// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        Array, ArrayData, ArrayDataBuilder, ArrayRef, BinaryArray, ListArray, StringArray,
        UInt64Array,
    },
    buffer::{MutableBuffer, NullBuffer},
    datatypes::Schema as ArrowSchema,
    record_batch::RecordBatch as ArrowRecordBatch,
    util::bit_util,
};
use common_types::{
    datum::DatumKind,
    schema::{ArrowSchemaRef, DataType, Field, Schema},
};
use common_util::error::BoxError;
use snafu::{Backtrace, ResultExt, Snafu};

use crate::sst::writer::{EncodeRecordBatch, Result};

//  hard coded in https://github.com/apache/arrow-rs/blob/20.0.0/arrow/src/array/array_list.rs#L185
const LIST_ITEM_NAME: &str = "item";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
    "Hybrid format only support variable length types UTF8 and Binary, current type:{:?}.\nBacktrace:\n{}",
    type_name,
    backtrace
    ))]
    VariableLengthType {
        type_name: DataType,
        backtrace: Backtrace,
    },
}

#[derive(Debug, Clone, Copy)]
struct SliceArg {
    offset: usize,
    length: usize,
}

/// ArrayHandle is used to keep different offsets of array, which can be
/// concatenated together.
///
/// Note:
/// 1. Array.slice(offset, length) don't work as expected, since the
/// underlying buffer is still shared without slice.
/// 2. Array should be [fixed-size primitive](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout)
#[derive(Debug, Clone)]
struct ArrayHandle {
    array: ArrayRef,
    slice_args: Vec<SliceArg>,
}

impl ArrayHandle {
    fn new(array: ArrayRef) -> Self {
        Self::with_slice_args(array, Vec::new())
    }

    fn with_slice_args(array: ArrayRef, slice_args: Vec<SliceArg>) -> Self {
        Self { array, slice_args }
    }

    fn append_slice_arg(&mut self, arg: SliceArg) {
        self.slice_args.push(arg)
    }

    fn len(&self) -> usize {
        self.slice_args.iter().map(|arg| arg.length).sum()
    }

    // Note: this require primitive array
    fn data_slice(&self) -> Vec<u8> {
        self.array.to_data().buffers()[0].as_slice().to_vec()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.array.nulls()
    }
}

/// `TsidBatch` is used to collect column data for the same TSID
#[derive(Debug)]
struct TsidBatch {
    non_collapsible_col_values: Vec<String>,
    // record_batch_idx -> ArrayHandle
    // Store collapsible data in multi record batch.
    // Vec<ArrayHandle> contains multi columns data.
    collapsible_col_arrays: BTreeMap<usize, Vec<ArrayHandle>>,
}

impl TsidBatch {
    fn new(non_collapsible_col_values: Vec<String>) -> Self {
        Self {
            non_collapsible_col_values,
            collapsible_col_arrays: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct IndexedType {
    pub idx: usize,
    pub data_type: DatumKind,
}

struct IndexedArray {
    idx: usize,
    array: ArrayRef,
}

/// Convert collapsible columns to list type
pub fn build_hybrid_arrow_schema(schema: &Schema) -> ArrowSchemaRef {
    let arrow_schema = schema.to_arrow_schema_ref();
    let new_fields = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if schema.is_collapsible_column(idx) {
                let field_type = DataType::List(Arc::new(Field::new(
                    LIST_ITEM_NAME,
                    field.data_type().clone(),
                    true,
                )));
                Arc::new(Field::new(field.name(), field_type, true))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new_with_metadata(
        new_fields,
        arrow_schema.metadata().clone(),
    ))
}

struct StringArrayWrapper<'a>(&'a StringArray);
struct BinaryArrayWrapper<'a>(&'a BinaryArray);

/// VariableSizeArray is a trait of variable-size array, such as StringArray and
/// BinaryArray. Dealing with the buffer data.
///
/// There is no common trait for variable-size array to dealing with the buffer
/// data in arrow-rs library.
trait VariableSizeArray {
    // Returns the offset values in the offsets buffer.
    fn value_offsets(&self) -> &[i32];
    // Returns the length for the element at index i.
    fn value_length(&self, index: usize) -> i32;
    // Returns a clone of the value data buffer.
    fn value_data(&self) -> &[u8];
}

macro_rules! impl_offsets {
    ($array: ty) => {
        impl<'a> VariableSizeArray for $array {
            fn value_offsets(&self) -> &[i32] {
                self.0.value_offsets()
            }

            fn value_length(&self, index: usize) -> i32 {
                self.0.value_length(index)
            }

            fn value_data(&self) -> &[u8] {
                self.0.value_data()
            }
        }
    };
}

impl_offsets!(StringArrayWrapper<'a>);
impl_offsets!(BinaryArrayWrapper<'a>);

/// ListArrayBuilder is used for concat slice of different Arrays represented by
/// ArrayHandle into one ListArray
struct ListArrayBuilder {
    datum_kind: DatumKind,
    // Vec<ArrayHandle> of row
    multi_row_arrays: Vec<Vec<ArrayHandle>>,
}

impl ListArrayBuilder {
    fn new(datum_kind: DatumKind, multi_row_arrays: Vec<Vec<ArrayHandle>>) -> Self {
        Self {
            datum_kind,
            multi_row_arrays,
        }
    }

    fn build_child_data(&self, offsets: &mut MutableBuffer) -> Result<ArrayData> {
        // Num of raw data in child data.
        let values_num = self
            .multi_row_arrays
            .iter()
            .map(|handles| handles.iter().map(|h| h.len()).sum::<usize>())
            .sum();

        // Initialize null_buffer with all 1, so we don't need to set it when array's
        // null_bitmap is None
        //
        // Note: bit set to 1 means value is not null.
        let mut null_buffer = new_ones_buffer(values_num);
        let null_slice = null_buffer.as_slice_mut();

        let mut length_so_far: i32 = 0;
        for arrays in &self.multi_row_arrays {
            for array_handle in arrays {
                let nulls = array_handle.nulls();

                for slice_arg in &array_handle.slice_args {
                    let offset = slice_arg.offset;
                    let length = slice_arg.length;
                    if let Some(nulls) = nulls {
                        // TODO: We now set bitmap one by one, a more complicated but efficient way
                        // is to operate on bitmap buffer bits directly,
                        // like what we do with values(slice and shift)
                        for i in 0..length {
                            if nulls.is_null(i + offset) {
                                bit_util::unset_bit(null_slice, length_so_far as usize + i);
                            }
                        }
                    }
                    length_so_far += length as i32;
                }
            }
        }

        let mut builder = ArrayData::builder(self.datum_kind.to_arrow_data_type())
            .len(values_num)
            .null_bit_buffer(Some(null_buffer.into()));

        builder = self.apply_child_data_buffer(builder, offsets)?;
        let values_array_data = builder.build().box_err().context(EncodeRecordBatch)?;

        Ok(values_array_data)
    }

    fn apply_child_data_buffer(
        &self,
        mut builder: ArrayDataBuilder,
        offsets: &mut MutableBuffer,
    ) -> Result<ArrayDataBuilder> {
        let (inner_offsets, values) = if let Some(data_type_size) = self.datum_kind.size() {
            (
                None,
                self.build_fixed_size_array_buffer(offsets, data_type_size),
            )
        } else {
            let (inner_offsets, values) = self.build_variable_size_array_buffer(offsets)?;
            (Some(inner_offsets), values)
        };

        if let Some(buffer) = inner_offsets {
            builder = builder.add_buffer(buffer.into());
        }
        builder = builder.add_buffer(values.into());
        Ok(builder)
    }

    fn build_fixed_size_array_buffer(
        &self,
        offsets: &mut MutableBuffer,
        data_type_size: usize,
    ) -> MutableBuffer {
        let mut length_so_far: i32 = 0;
        offsets.push(length_so_far);

        let values_num: usize = self
            .multi_row_arrays
            .iter()
            .map(|handles| handles.iter().map(|handle| handle.len()).sum::<usize>())
            .sum();
        let mut values = MutableBuffer::new(values_num * data_type_size);
        for arrays in &self.multi_row_arrays {
            for array_handle in arrays {
                let shared_buffer = array_handle.data_slice();
                for slice_arg in &array_handle.slice_args {
                    let offset = slice_arg.offset;
                    let length = slice_arg.length;
                    length_so_far += length as i32;

                    values.extend_from_slice(
                        &shared_buffer[offset * data_type_size..(offset + length) * data_type_size],
                    );
                }
            }
            // The data in the arrays belong to the same tsid, so the offsets is the total
            // len.
            offsets.push(length_so_far);
        }

        values
    }

    /// Return (offsets_buffer, values_buffer) according to arrow
    /// `Variable-size Binary Layout`. Refer to https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout.
    fn build_variable_size_array_buffer(
        &self,
        offsets: &mut MutableBuffer,
    ) -> Result<(MutableBuffer, MutableBuffer)> {
        let mut length_so_far: i32 = 0;
        offsets.push(length_so_far);

        let (offsets_length_total, values_length_total) =
            self.compute_variable_size_array_buffer_length()?;

        let mut inner_values = MutableBuffer::new(values_length_total);
        let mut inner_offsets = MutableBuffer::new(offsets_length_total);

        self.build_variable_size_array_buffer_data(
            &mut length_so_far,
            offsets,
            &mut inner_offsets,
            &mut inner_values,
        )?;
        Ok((inner_offsets, inner_values))
    }

    fn convert_to_variable_size_array<'a>(
        &self,
        array_handle: &'a ArrayHandle,
    ) -> Result<Box<dyn VariableSizeArray + 'a>> {
        match self.datum_kind.to_arrow_data_type() {
            DataType::Utf8 => Ok(Box::new(StringArrayWrapper(
                array_handle
                    .array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("downcast StringArray failed"),
            ))),
            DataType::Binary => Ok(Box::new(BinaryArrayWrapper(
                array_handle
                    .array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("downcast BinaryArray failed"),
            ))),
            typ => VariableLengthType { type_name: typ }
                .fail()
                .box_err()
                .context(EncodeRecordBatch),
        }
    }

    /// Return (offsets_length_total, values_length_total).
    #[inline]
    fn compute_variable_size_array_buffer_length(&self) -> Result<(usize, usize)> {
        let mut offsets_length_total = 0;
        let mut values_length_total = 0;

        for multi_row_array in &self.multi_row_arrays {
            for array_handle in multi_row_array {
                let array = self.convert_to_variable_size_array(array_handle)?;
                for slice_arg in &array_handle.slice_args {
                    let start = array.value_offsets()[slice_arg.offset];
                    let end = array.value_offsets()[slice_arg.offset + slice_arg.length];

                    offsets_length_total += slice_arg.length;
                    values_length_total += (end - start) as usize;
                }
            }
        }

        Ok((offsets_length_total, values_length_total))
    }

    /// Build variable-size array buffer data.
    ///
    /// length_so_far and offsets are used for father buffer data.
    /// inner_offsets and inner_values are used for child buffer data.
    fn build_variable_size_array_buffer_data(
        &self,
        length_so_far: &mut i32,
        offsets: &mut MutableBuffer,
        inner_offsets: &mut MutableBuffer,
        inner_values: &mut MutableBuffer,
    ) -> Result<()> {
        let mut inner_length_so_far: i32 = 0;
        inner_offsets.push(inner_length_so_far);

        for arrays in &self.multi_row_arrays {
            for array_handle in arrays {
                let array = self.convert_to_variable_size_array(array_handle)?;

                for slice_arg in &array_handle.slice_args {
                    *length_so_far += slice_arg.length as i32;

                    let start = array.value_offsets()[slice_arg.offset];
                    let end = array.value_offsets()[slice_arg.offset + slice_arg.length];

                    for i in slice_arg.offset..(slice_arg.offset + slice_arg.length) {
                        inner_length_so_far += array.value_length(i);
                        inner_offsets.push(inner_length_so_far);
                    }

                    inner_values
                        .extend_from_slice(&array.value_data()[start as usize..end as usize]);
                }
            }
            // The data in the arrays belong to the same tsid, so the offsets is the total
            // len.
            offsets.push(*length_so_far);
        }

        Ok(())
    }

    /// This function is a translation of [GenericListArray.from_iter_primitive](https://docs.rs/arrow/20.0.0/src/arrow/array/array_list.rs.html#151)
    fn build(self) -> Result<ListArray> {
        // The data in multi_row_arrays belong to different tsids.
        // So the values num is the len of multi_row_arrays.
        let array_len = self.multi_row_arrays.len();
        let mut offsets = MutableBuffer::new(array_len * std::mem::size_of::<i32>());
        let child_data = self.build_child_data(&mut offsets)?;
        let field = Arc::new(Field::new(
            LIST_ITEM_NAME,
            self.datum_kind.to_arrow_data_type(),
            true,
        ));
        let array_data = ArrayData::builder(DataType::List(field))
            .len(array_len)
            .add_buffer(offsets.into())
            .add_child_data(child_data);

        // TODO: change to unsafe version?
        // https://docs.rs/arrow/20.0.0/src/arrow/array/array_list.rs.html#192
        // let array_data = unsafe { array_data.build_unchecked() };
        let array_data = array_data.build().box_err().context(EncodeRecordBatch)?;

        Ok(ListArray::from(array_data))
    }
}

/// Builds hybrid record by concat timestamp and non key columns into
/// `ListArray`.
fn build_hybrid_record(
    arrow_schema: ArrowSchemaRef,
    tsid_type: &IndexedType,
    non_collapsible_col_types: &[IndexedType],
    collapsible_col_types: &[IndexedType],
    // tsid -> TsidBatch
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_array = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned());

    // col_idx -> tsid -> data array
    let mut collapsible_col_arrays =
        vec![vec![Vec::new(); tsid_array.len()]; collapsible_col_types.len()];
    let mut non_collapsible_col_arrays = vec![Vec::new(); non_collapsible_col_types.len()];

    // Reorganize data in batch_by_tsid.
    // tsid-> col_idx-> data array ==> col_idx -> tsid -> data array
    // example:
    // tsid0 -> vec![ data_arrays0 of col0, data_array1 of col1]
    // tsid1 -> vec![ data_arrays2 of col0, data_array3 of col1]
    // ==>
    // col0 -> vec![ data_arrays0 of tsid0, data_array2 of tsid1]
    // col1 -> vec![ data_arrays1 of tsid0, data_array3 of tsid1]
    for (tsid_idx, batch) in batch_by_tsid.into_values().enumerate() {
        for col_array in batch.collapsible_col_arrays.into_values() {
            for (col_idx, arr) in col_array.into_iter().enumerate() {
                collapsible_col_arrays[col_idx][tsid_idx].push(arr);
            }
        }
        for (col_idx, arr) in batch.non_collapsible_col_values.into_iter().enumerate() {
            non_collapsible_col_arrays[col_idx].push(arr);
        }
    }
    let tsid_array = IndexedArray {
        idx: tsid_type.idx,
        array: Arc::new(tsid_array),
    };
    let non_collapsible_col_arrays = non_collapsible_col_arrays
        .into_iter()
        .zip(non_collapsible_col_types.iter().map(|n| n.idx))
        .map(|(c, idx)| IndexedArray {
            idx,
            array: Arc::new(StringArray::from(c)) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let collapsible_col_arrays = collapsible_col_arrays
        .into_iter()
        .zip(collapsible_col_types.iter().map(|n| (n.idx, n.data_type)))
        .map(|(handle, (idx, datum_type))| {
            Ok(IndexedArray {
                idx,
                array: Arc::new(ListArrayBuilder::new(datum_type, handle).build()?),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let all_columns = [
        vec![tsid_array],
        non_collapsible_col_arrays,
        collapsible_col_arrays,
    ]
    .into_iter()
    .flatten()
    .map(|indexed_array| (indexed_array.idx, indexed_array.array))
    .collect::<BTreeMap<_, _>>()
    .into_values()
    .collect::<Vec<_>>();

    ArrowRecordBatch::try_new(arrow_schema, all_columns)
        .box_err()
        .context(EncodeRecordBatch)
}

/// Converts arrow record batch into hybrid record format describe in
/// `StorageFormat::Hybrid`
pub fn convert_to_hybrid_record(
    tsid_type: &IndexedType,
    non_collapsible_col_types: &[IndexedType],
    collapsible_col_types: &[IndexedType],
    hybrid_arrow_schema: ArrowSchemaRef,
    arrow_record_batches: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    // TODO: should keep tsid ordering here?
    let mut batch_by_tsid = BTreeMap::new();
    for (record_idx, record_batch) in arrow_record_batches.iter().enumerate() {
        let tsid_array = record_batch
            .column(tsid_type.idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked when create table");

        if tsid_array.is_empty() {
            continue;
        }

        let non_collapsible_col_values = non_collapsible_col_types
            .iter()
            .map(|col| {
                record_batch
                    .column(col.idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("checked in HybridRecordEncoder::try_new")
            })
            .collect::<Vec<_>>();
        let mut previous_tsid = tsid_array.value(0);
        // duplicated_tsids is an array of every tsid's offset in origin array
        // the length of each tsid can be calculated with
        // tsid_n = duplicated_tsids[n+1].offset - duplicated_tsids[n].offset
        let mut duplicated_tsids = vec![(previous_tsid, 0)]; // (tsid, offset)
        for row_idx in 1..tsid_array.len() {
            let tsid = tsid_array.value(row_idx);
            if tsid != previous_tsid {
                previous_tsid = tsid;
                duplicated_tsids.push((tsid, row_idx));
            }
        }
        for i in 0..duplicated_tsids.len() {
            let (tsid, offset) = duplicated_tsids[i];
            let length = if i == duplicated_tsids.len() - 1 {
                tsid_array.len() - offset
            } else {
                duplicated_tsids[i + 1].1 - offset
            };

            let batch = batch_by_tsid.entry(tsid).or_insert_with(|| {
                TsidBatch::new(
                    non_collapsible_col_values
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect(),
                )
            });
            let collapsible_col_arrays = batch
                .collapsible_col_arrays
                .entry(record_idx)
                .or_insert_with(|| {
                    collapsible_col_types
                        .iter()
                        .map(|col| ArrayHandle::new(record_batch.column(col.idx).clone()))
                        .collect()
                });
            // Append the slice arg to all columns in the same record batch.
            for handle in collapsible_col_arrays {
                handle.append_slice_arg(SliceArg { offset, length });
            }
        }
    }
    build_hybrid_record(
        hybrid_arrow_schema,
        tsid_type,
        non_collapsible_col_types,
        collapsible_col_types,
        batch_by_tsid,
    )
}

/// Return a MutableBuffer with bits all set to 1
pub fn new_ones_buffer(len: usize) -> MutableBuffer {
    let null_buffer = MutableBuffer::new_null(len);
    let buf_cap = null_buffer.capacity();
    null_buffer.with_bitset(buf_cap, true)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{TimestampMillisecondArray, UInt16Array},
        buffer::Buffer,
        datatypes::{TimestampMillisecondType, UInt16Type},
    };

    use super::*;

    impl From<(usize, usize)> for SliceArg {
        fn from(offset_length: (usize, usize)) -> Self {
            Self {
                offset: offset_length.0,
                length: offset_length.1,
            }
        }
    }

    fn timestamp_array(start: i64, end: i64) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from_iter_values(start..end))
    }

    fn uint16_array(values: Vec<Option<u16>>) -> ArrayRef {
        let arr: UInt16Array = values.into_iter().collect();

        Arc::new(arr)
    }

    fn string_array(values: Vec<Option<&str>>) -> ArrayRef {
        let arr: StringArray = values.into_iter().collect();

        Arc::new(arr)
    }

    #[test]
    fn merge_timestamp_array_to_list() {
        let row0 = vec![ArrayHandle::with_slice_args(
            timestamp_array(1, 20),
            vec![(1, 2).into(), (10, 3).into()],
        )];
        let row1 = vec![ArrayHandle::with_slice_args(
            timestamp_array(100, 120),
            vec![(1, 2).into(), (10, 3).into()],
        )];

        let data = vec![
            Some(vec![Some(2), Some(3), Some(11), Some(12), Some(13)]),
            Some(vec![Some(101), Some(102), Some(110), Some(111), Some(112)]),
        ];
        let expected = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(data);
        let list_array = ListArrayBuilder::new(DatumKind::Timestamp, vec![row0, row1])
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }

    #[test]
    fn merge_u16_array_with_none_to_list() {
        let row0 = vec![ArrayHandle::with_slice_args(
            uint16_array(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ]),
            vec![(1, 3).into(), (4, 1).into()],
        )];
        let row1 = vec![ArrayHandle::with_slice_args(
            uint16_array(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ]),
            vec![(0, 1).into()],
        )];

        let data = vec![
            Some(vec![Some(2), None, Some(3), Some(4)]),
            Some(vec![Some(1)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt16Type, _, _>(data);
        let list_array = ListArrayBuilder::new(DatumKind::UInt16, vec![row0, row1])
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }

    #[test]
    fn merge_string_array_with_none_to_list() {
        let row0 = vec![ArrayHandle::with_slice_args(
            string_array(vec![
                Some("a"),
                Some("bb"),
                None,
                Some("ccc"),
                Some("d"),
                Some("eeee"),
                Some("eee"),
            ]),
            vec![(1, 3).into(), (5, 1).into()],
        )];

        let row1 = vec![ArrayHandle::with_slice_args(
            string_array(vec![
                Some("a"),
                Some("bb"),
                None,
                Some("ccc"),
                Some("d"),
                Some("eeee"),
                Some("eee"),
            ]),
            vec![(0, 1).into()],
        )];

        let string_data =
            string_array(vec![Some("bb"), None, Some("ccc"), Some("eeee"), Some("a")]);
        let offsets: [i32; 3] = [0, 4, 5];
        let array_data = ArrayData::builder(DataType::List(Arc::new(Field::new(
            LIST_ITEM_NAME,
            DataType::Utf8,
            true,
        ))))
        .len(2)
        .add_buffer(Buffer::from_slice_ref(offsets))
        .add_child_data(string_data.to_data())
        .build()
        .unwrap();
        let expected = ListArray::from(array_data);
        let list_array = ListArrayBuilder::new(DatumKind::String, vec![row0, row1])
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }

    // Fix https://github.com/CeresDB/ceresdb/issues/255
    // null buffer will ceiled by 8, so its capacity may less than `size`
    #[test]
    fn new_null_buffer_with_different_size() {
        let sizes = [1, 8, 11, 20, 511];

        for size in &sizes {
            let _ = new_ones_buffer(*size);
        }
    }
}
