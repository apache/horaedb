// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, sync::Arc};

use arrow_deps::arrow::{
    array::{Array, ArrayData, ArrayRef, ListArray, StringArray, UInt64Array},
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
    positions: Vec<(usize, usize)>, // (offset ,length)
}

impl ArrayHandle {
    fn new(array: ArrayRef) -> Self {
        Self::with_positions(array, Vec::new())
    }

    fn with_positions(array: ArrayRef, positions: Vec<(usize, usize)>) -> Self {
        Self { array, positions }
    }

    fn append_pos(&mut self, offset: usize, length: usize) {
        self.positions.push((offset, length))
    }

    fn len(&self) -> usize {
        self.positions.iter().map(|(_, len)| len).sum()
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
    key_values: Vec<String>,
    timestamp_array: ArrayHandle,
    non_key_arrays: Vec<ArrayHandle>,
}

impl TsidBatch {
    fn new(key_values: Vec<String>, timestamp: ArrayRef, non_key_arrays: Vec<ArrayRef>) -> Self {
        Self {
            key_values,
            timestamp_array: ArrayHandle::new(timestamp),
            non_key_arrays: non_key_arrays
                .into_iter()
                .map(|f| ArrayHandle::new(f))
                .collect(),
        }
    }

    fn append_postition(&mut self, offset: usize, length: usize) {
        self.timestamp_array.append_pos(offset, length);
        for handle in &mut self.non_key_arrays {
            handle.append_pos(offset, length);
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

fn is_collapsable_column(idx: usize, timestamp_idx: usize, non_key_column_idxes: &[usize]) -> bool {
    idx == timestamp_idx || non_key_column_idxes.contains(&idx)
}

/// Convert timestamp/non key columns to list type
pub fn build_hybrid_arrow_schema(
    timestamp_idx: usize,
    non_key_column_idxes: Vec<usize>,
    schema: &Schema,
) -> ArrowSchemaRef {
    let arrow_schema = schema.to_arrow_schema_ref();
    let new_fields = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if is_collapsable_column(idx, timestamp_idx, &non_key_column_idxes) {
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
        let data_type_size = self
            .datum_kind
            .size()
            .expect("checked in HybridRecordEncoder::try_new");
        let values_num = self.list_of_arrays.iter().map(|handle| handle.len()).sum();
        let mut values = MutableBuffer::new(values_num * data_type_size);
        let mut null_buffer = MutableBuffer::new_null(values_num);
        let null_slice = null_buffer.as_slice_mut();

        let mut length_so_far: i32 = 0;
        offsets.push(length_so_far);
        for array_handle in &self.list_of_arrays {
            let shared_buffer = array_handle.data_slice();
            let null_bitmap = array_handle.null_bitmap();

            for (offset, length) in &array_handle.positions {
                if let Some(bitmap) = null_bitmap {
                    for i in 0..*length {
                        if bitmap.is_set(i + *offset) {
                            bit_util::set_bit(null_slice, length_so_far as usize + i);
                        }
                    }
                } else {
                    for i in 0..*length {
                        bit_util::set_bit(null_slice, length_so_far as usize + i);
                    }
                }
                length_so_far += *length as i32;
                values.extend_from_slice(
                    &shared_buffer[offset * data_type_size..(offset + length) * data_type_size],
                );
            }
            offsets.push(length_so_far);
        }
        debug!(
            "build_child_data offsets:{:?}, values:{:?}",
            offsets.as_slice(),
            values.as_slice()
        );

        let values_array_data = ArrayData::builder(self.datum_kind.to_arrow_data_type())
            .len(values_num)
            .add_buffer(values.into())
            .null_bit_buffer(Some(null_buffer.into()))
            .build()
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch)?;

        Ok(values_array_data)
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
    timestamp_type: &IndexedType,
    key_types: &[IndexedType],
    non_key_types: &[IndexedType],
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_array = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned());
    let mut timestamp_array = Vec::new();
    let mut non_key_column_arrays = vec![Vec::new(); non_key_types.len()];
    let mut key_column_arrays = vec![Vec::new(); key_types.len()];

    for batch in batch_by_tsid.into_values() {
        timestamp_array.push(batch.timestamp_array);
        for (idx, handle) in batch.non_key_arrays.into_iter().enumerate() {
            non_key_column_arrays[idx].push(handle);
        }
        for (idx, tagv) in batch.key_values.into_iter().enumerate() {
            key_column_arrays[idx].push(tagv);
        }
    }
    let tsid_array = IndexedArray {
        idx: tsid_type.idx,
        array: Arc::new(tsid_array),
    };
    let timestamp_array = IndexedArray {
        idx: timestamp_type.idx,
        array: Arc::new(ListArrayBuilder::new(timestamp_type.data_type, timestamp_array).build()?),
    };
    let key_column_arrays = key_column_arrays
        .into_iter()
        .zip(key_types.iter().map(|n| n.idx))
        .map(|(c, idx)| IndexedArray {
            idx,
            array: Arc::new(StringArray::from(c)) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let non_key_column_arrays = non_key_column_arrays
        .into_iter()
        .zip(non_key_types.iter().map(|n| (n.idx, n.data_type)))
        .map(|(handle, (idx, datum_type))| {
            Ok(IndexedArray {
                idx,
                array: Arc::new(ListArrayBuilder::new(datum_type, handle).build()?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let all_columns = [
        vec![tsid_array, timestamp_array],
        key_column_arrays,
        non_key_column_arrays,
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
    timestamp_type: &IndexedType,
    key_types: &[IndexedType],
    non_key_types: &[IndexedType],
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

        let key_values = key_types
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
                    key_values
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect(),
                    record_batch.column(timestamp_type.idx).clone(),
                    non_key_types
                        .iter()
                        .map(|col| record_batch.column(col.idx).clone())
                        .collect(),
                )
            });
            batch.append_postition(offset, length);
        }
    }

    build_hybrid_record(
        hybrid_arrow_schema,
        tsid_type,
        timestamp_type,
        key_types,
        non_key_types,
        batch_by_tsid,
    )
}

#[cfg(test)]
mod tests {
    use arrow_deps::arrow::{
        array::{TimestampMillisecondArray, UInt16Array},
        datatypes::{TimestampMillisecondType, UInt16Type},
    };

    use super::*;

    fn timestamp_array(start: i64, end: i64) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from_iter_values(start..end))
    }

    fn uint16_array(values: Vec<Option<u16>>) -> ArrayRef {
        let arr: UInt16Array = values.into_iter().collect();

        Arc::new(arr)
    }

    #[test]
    fn merge_timestamp_array_to_list() {
        let list_of_arrays = vec![
            ArrayHandle::with_positions(timestamp_array(1, 20), vec![(1, 2), (10, 3)]),
            ArrayHandle::with_positions(timestamp_array(100, 120), vec![(1, 2), (10, 3)]),
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
        let list_of_arrays = vec![ArrayHandle::with_positions(
            uint16_array(vec![
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ]),
            vec![(1, 3), (4, 1)],
        )];

        let data = vec![Some(vec![Some(2), None, Some(3), Some(4)])];
        let expected = ListArray::from_iter_primitive::<UInt16Type, _, _>(data);
        let list_array = ListArrayBuilder::new(DatumKind::UInt16, list_of_arrays)
            .build()
            .unwrap();

        assert_eq!(list_array, expected);
    }
}
