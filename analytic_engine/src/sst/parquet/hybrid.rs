// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, sync::Arc};

use arrow_deps::arrow::{
    array::{
        Array, ArrayData, ArrayDataBuilder, ArrayRef, BinaryArray, ListArray, StringArray,
        UInt64Array,
    },
    bitmap::Bitmap,
    buffer::MutableBuffer,
    datatypes::Schema as ArrowSchema,
    record_batch::RecordBatch as ArrowRecordBatch,
    util::bit_util,
};
use common_types::{
    datum::DatumKind,
    schema::{ArrowSchemaRef, DataType, Field, Schema},
};
use log::debug;
use snafu::ResultExt;

use crate::sst::builder::{EncodeRecordBatch, Result};

//  hard coded in https://github.com/apache/arrow-rs/blob/20.0.0/arrow/src/array/array_list.rs#L185
const LIST_ITEM_NAME: &str = "item";

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
/// 2. Array shoule be [fixed-size primitive](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout)
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
    fn data_slice(&self) -> &[u8] {
        self.array.data().buffers()[0].as_slice()
    }

    fn null_bitmap(&self) -> Option<&Bitmap> {
        self.array.data().null_bitmap()
    }
}

/// `TsidBatch` is used to collect column data for the same TSID
#[derive(Debug)]
struct TsidBatch {
    non_collapsible_col_values: Vec<String>,
    collapsible_col_arrays: Vec<ArrayHandle>,
}

impl TsidBatch {
    fn new(non_collapsible_col_values: Vec<String>, collapsible_col_arrays: Vec<ArrayRef>) -> Self {
        Self {
            non_collapsible_col_values,
            collapsible_col_arrays: collapsible_col_arrays
                .into_iter()
                .map(|f| ArrayHandle::new(f))
                .collect(),
        }
    }

    fn append_slice_arg(&mut self, arg: SliceArg) {
        for handle in &mut self.collapsible_col_arrays {
            handle.append_slice_arg(arg);
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
                let field_type = DataType::List(Box::new(Field::new(
                    LIST_ITEM_NAME,
                    field.data_type().clone(),
                    true,
                )));
                Field::new(field.name(), field_type, true)
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

/// ListArrayBuilder is used for concat slice of different Arrays represented by
/// ArrayHandle into one ListArray
struct ListArrayBuilder {
    datum_kind: DatumKind,
    list_of_arrays: Vec<ArrayHandle>,
}

impl ListArrayBuilder {
    fn new(datum_kind: DatumKind, list_of_arrays: Vec<ArrayHandle>) -> Self {
        Self {
            datum_kind,
            list_of_arrays,
        }
    }

    fn build_child_data(&self, offsets: &mut MutableBuffer) -> Result<ArrayData> {
        let values_num = self.list_of_arrays.iter().map(|handle| handle.len()).sum();

        // Initialize null_buffer with all 1, so we don't need to set it when array's
        // null_bitmap is None
        //
        // Note: bit set to 1 means value is not null.
        let mut null_buffer = MutableBuffer::new_null(values_num).with_bitset(values_num, true);
        let null_slice = null_buffer.as_slice_mut();

        let mut length_so_far: i32 = 0;

        for array_handle in &self.list_of_arrays {
            let shared_buffer = array_handle.data_slice();
            let null_bitmap = array_handle.null_bitmap();

            for slice_arg in &array_handle.slice_args {
                let offset = slice_arg.offset;
                let length = slice_arg.length;
                if let Some(bitmap) = null_bitmap {
                    // TODO: We now set bitmap one by one, a more complicated but efficient way is
                    // to operate on bitmap buffer bits directly, like what we do
                    // with values(slice and shift)
                    for i in 0..length {
                        if !bitmap.is_set(i + offset) {
                            bit_util::unset_bit(null_slice, length_so_far as usize + i);
                        }
                    }
                }
                length_so_far += length as i32;
            }
        }

        let mut builder = ArrayData::builder(self.datum_kind.to_arrow_data_type())
            .len(values_num)
            .null_bit_buffer(Some(null_buffer.into()));

        builder = self.build_buffer(builder, offsets);
        let values_array_data = builder
            .build()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(values_array_data)
    }

    fn build_buffer(
        &self,
        mut builder: ArrayDataBuilder,
        offsets: &mut MutableBuffer,
    ) -> ArrayDataBuilder {
        let mut length_so_far: i32 = 0;
        offsets.push(length_so_far);

        let (inner_offsets, values) = if let Some(data_type_size) = self.datum_kind.size() {
            let values_num: usize = self.list_of_arrays.iter().map(|handle| handle.len()).sum();

            let mut values = MutableBuffer::new(values_num * data_type_size);
            for array_handle in &self.list_of_arrays {
                let shared_buffer = array_handle.data_slice();

                for slice_arg in &array_handle.slice_args {
                    let offset = slice_arg.offset;
                    let length = slice_arg.length;
                    length_so_far += length as i32;

                    values.extend_from_slice(
                        &shared_buffer[offset * data_type_size..(offset + length) * data_type_size],
                    );
                }
                offsets.push(length_so_far);
            }
            (None, values)
        } else {
            let mut values = MutableBuffer::new(0);

            let mut inner_length_so_far: i32 = 0;
            let mut inner_offsets = MutableBuffer::new(0);
            inner_offsets.push(inner_length_so_far);
            for array_handle in &self.list_of_arrays {
                match self.datum_kind.to_arrow_data_type() {
                    DataType::Utf8 => {
                        let string_array = StringArray::from(array_handle.array.data().clone());
                        for slice_arg in &array_handle.slice_args {
                            let offset = slice_arg.offset;
                            let length = slice_arg.length;
                            length_so_far += length as i32;

                            let start = string_array.value_offsets()[offset];
                            let end = string_array.value_offsets()[offset + length];

                            for i in (offset as usize)..(offset + length as usize) {
                                inner_length_so_far += string_array.value_length(i);
                                inner_offsets.push(inner_length_so_far);
                            }

                            values.extend_from_slice(
                                &array_handle.array.data().buffers()[1].as_slice()
                                    [start as usize..end as usize],
                            );
                        }
                        offsets.push(length_so_far);
                    }
                    DataType::Binary => {
                        let binary_array = BinaryArray::from(array_handle.array.data().clone());
                        for slice_arg in &array_handle.slice_args {
                            let offset = slice_arg.offset;
                            let length = slice_arg.length;
                            length_so_far += length as i32;

                            let start = binary_array.value_offsets()[offset];
                            let end = binary_array.value_offsets()[offset + length];

                            for i in (offset as usize)..(offset + length as usize) {
                                inner_length_so_far += binary_array.value_length(i);
                                inner_offsets.push(inner_length_so_far);
                            }

                            values.extend_from_slice(
                                &array_handle.array.data().buffers()[1].as_slice()
                                    [start as usize..end as usize],
                            );
                        }
                        offsets.push(length_so_far);
                    }
                    _ => panic!("Only support Utf8 and Binary"),
                }
            }
            (Some(inner_offsets), values)
        };
        if let Some(buffer) = inner_offsets {
            builder = builder.add_buffer(buffer.into());
        }
        builder = builder.add_buffer(values.into());
        builder
    }

    /// This function is a translation of [GenericListArray.from_iter_primitive](https://docs.rs/arrow/20.0.0/src/arrow/array/array_list.rs.html#151)
    fn build(self) -> Result<ListArray> {
        let array_len = self.list_of_arrays.len();
        let mut offsets =
            MutableBuffer::new(self.list_of_arrays.len() * std::mem::size_of::<i32>());
        let child_data = self.build_child_data(&mut offsets)?;
        let field = Box::new(Field::new(
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
        let array_data = array_data
            .build()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(ListArray::from(array_data))
    }
}

/// Builds hybrid record by concat timestamp and non key columns into
/// `ListArray`
fn build_hybrid_record(
    arrow_schema: ArrowSchemaRef,
    tsid_type: &IndexedType,
    non_collapsible_col_types: &[IndexedType],
    collapsible_col_types: &[IndexedType],
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_array = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned());
    let mut collapsible_col_arrays = vec![Vec::new(); collapsible_col_types.len()];
    let mut non_collapsible_col_arrays = vec![Vec::new(); non_collapsible_col_types.len()];

    for batch in batch_by_tsid.into_values() {
        for (idx, arr) in batch.collapsible_col_arrays.into_iter().enumerate() {
            collapsible_col_arrays[idx].push(arr);
        }
        for (idx, arr) in batch.non_collapsible_col_values.into_iter().enumerate() {
            non_collapsible_col_arrays[idx].push(arr);
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
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)
}

/// Converts arrow record batch into hybrid record format describe in
/// `StorageFormat::Hybrid`
pub fn convert_to_hybrid_record(
    tsid_type: &IndexedType,
    non_collapsible_col_types: &[IndexedType],
    collapsible_col_types: &[IndexedType],
    hybrid_arrow_schema: ArrowSchemaRef,
    arrow_record_batchs: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    // TODO: should keep tsid ordering here?
    let mut batch_by_tsid = BTreeMap::new();
    for record_batch in arrow_record_batchs {
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
                    collapsible_col_types
                        .iter()
                        .map(|col| record_batch.column(col.idx).clone())
                        .collect(),
                )
            });
            batch.append_slice_arg(SliceArg { offset, length });
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

#[cfg(test)]
mod tests {
    use arrow_deps::arrow::{
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
        let list_of_arrays = vec![
            ArrayHandle::with_slice_args(
                timestamp_array(1, 20),
                vec![(1, 2).into(), (10, 3).into()],
            ),
            ArrayHandle::with_slice_args(
                timestamp_array(100, 120),
                vec![(1, 2).into(), (10, 3).into()],
            ),
        ];

        let data = vec![
            Some(vec![Some(2), Some(3), Some(11), Some(12), Some(13)]),
            Some(vec![Some(101), Some(102), Some(110), Some(111), Some(112)]),
        ];
        let expected = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(data);
        let list_array = ListArrayBuilder::new(DatumKind::Timestamp, list_of_arrays)
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }

    #[test]
    fn merge_u16_array_with_none_to_list() {
        let list_of_arrays = vec![
            ArrayHandle::with_slice_args(
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
            ),
            ArrayHandle::with_slice_args(
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
            ),
        ];

        let data = vec![
            Some(vec![Some(2), None, Some(3), Some(4)]),
            Some(vec![Some(1)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt16Type, _, _>(data);
        let list_array = ListArrayBuilder::new(DatumKind::UInt16, list_of_arrays)
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }

    #[test]
    fn merge_string_array_with_none_to_list() {
        let list_of_arrays = vec![
            ArrayHandle::with_slice_args(
                string_array(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("c"),
                    Some("d"),
                    Some("e"),
                    Some("e"),
                ]),
                vec![(1, 3).into(), (4, 1).into()],
            ),
            ArrayHandle::with_slice_args(
                string_array(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("c"),
                    Some("d"),
                    Some("e"),
                    Some("e"),
                ]),
                vec![(0, 1).into()],
            ),
        ];

        // let data = vec![Some(vec![Some("b"), None, Some("c"), Some("d")])];
        // let expected =StringArray::from(data);
        let string_data = string_array(vec![Some("b"), None, Some("c"), Some("d"), Some("a")]);
        let offsets: [i32; 3] = [0, 4, 5];
        let array_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            LIST_ITEM_NAME,
            DataType::Utf8,
            true,
        ))))
        .len(2)
        .add_buffer(Buffer::from_slice_ref(&offsets))
        .add_child_data(string_data.data().to_owned())
        .build()
        .unwrap();
        let expected = ListArray::from(array_data);
        let list_array = ListArrayBuilder::new(DatumKind::String, list_of_arrays)
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }
}
