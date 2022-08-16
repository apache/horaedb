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
use snafu::{OptionExt, ResultExt};

use crate::sst::builder::{EncodeRecordBatch, Result, VariableLengthType};

//  hard coded in https://github.com/apache/arrow-rs/blob/20.0.0/arrow/src/array/array_list.rs#L185
const LIST_ITEM_NAME: &str = "item";

/// ArrayHandle is used to keep different offsets of array, which can be concat
/// together later.
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
    tag_values: Vec<String>,
    timestamp_handle: ArrayHandle,
    field_handles: Vec<ArrayHandle>,
}

impl TsidBatch {
    fn new(tag_values: Vec<String>, timestamp: ArrayRef, fields: Vec<ArrayRef>) -> Self {
        Self {
            tag_values,
            timestamp_handle: ArrayHandle::new(timestamp),
            field_handles: fields.into_iter().map(|f| ArrayHandle::new(f)).collect(),
        }
    }

    fn append_postition(&mut self, offset: usize, length: usize) {
        self.timestamp_handle.append_pos(offset, length);
        for handle in &mut self.field_handles {
            handle.append_pos(offset, length);
        }
    }
}

#[derive(Debug)]
struct IndexedKind {
    idx: usize,
    kind: DatumKind,
}

struct IndexedArray {
    idx: usize,
    array: ArrayRef,
}

/// Convert timestamp/fields column to list type
pub fn build_hybrid_arrow_schema(tsid_idx: usize, schema: &Schema) -> ArrowSchemaRef {
    let mut tag_idxes = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_idxes.push(idx)
        }
    }
    let arrow_schema = schema.to_arrow_schema_ref();
    let new_fields = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if idx == tsid_idx || tag_idxes.contains(&idx) {
                field.clone()
            } else {
                Field::new(
                    field.name(),
                    DataType::List(Box::new(Field::new(
                        LIST_ITEM_NAME,
                        field.data_type().clone(),
                        true,
                    ))),
                    true,
                )
            }
        })
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new_with_metadata(
        new_fields,
        arrow_schema.metadata().clone(),
    ))
}

fn merge_array_vec_to_list_array(
    datum_kind: DatumKind,
    list_of_arrays: Vec<ArrayHandle>,
) -> Result<ListArray> {
    assert!(!list_of_arrays.is_empty());

    let data_type_size = datum_kind.size().context(VariableLengthType {
        type_str: datum_kind.to_string(),
    })?;
    let array_len = list_of_arrays.len();
    let data_type = datum_kind.to_arrow_data_type();
    let values_num = list_of_arrays.iter().map(|handle| handle.len()).sum();
    let mut values = MutableBuffer::new(values_num * data_type_size);
    let mut null_buffer = MutableBuffer::new_null(values_num);
    let null_slice = null_buffer.as_slice_mut();
    let mut offsets = MutableBuffer::new(list_of_arrays.len() * std::mem::size_of::<i32>());
    let mut length_so_far: i32 = 0;
    offsets.push(length_so_far);

    for array_handle in list_of_arrays {
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
        "merge_array_vec_to_list offsets:{:?},values:{:?}",
        offsets.as_slice(),
        values.as_slice()
    );

    let values_array_data = ArrayData::builder(data_type.clone())
        .len(values_num)
        .add_buffer(values.into())
        .null_bit_buffer(Some(null_buffer.into()))
        .build()
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)?;
    let field = Box::new(Field::new(LIST_ITEM_NAME, data_type, true));
    let array_data = ArrayData::builder(DataType::List(field))
        .len(array_len)
        .add_buffer(offsets.into())
        .add_child_data(values_array_data);

    // TODO: change to unsafe version?
    // https://docs.rs/arrow/20.0.0/src/arrow/array/array_list.rs.html#192
    // let array_data = unsafe { array_data.build_unchecked() };
    let array_data = array_data
        .build()
        .map_err(|e| Box::new(e) as _)
        .context(EncodeRecordBatch)?;
    Ok(ListArray::from(array_data))
}

fn build_hybrid_record(
    arrow_schema: ArrowSchemaRef,
    tsid_kind: IndexedKind,
    timestamp_kind: IndexedKind,
    tag_kinds: Vec<IndexedKind>,
    field_kinds: Vec<IndexedKind>,
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_col = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned());
    let mut timestamp_handle = Vec::new();
    let mut field_handles = vec![Vec::new(); field_kinds.len()];
    let mut tag_handles = vec![Vec::new(); tag_kinds.len()];

    for batch in batch_by_tsid.into_values() {
        timestamp_handle.push(batch.timestamp_handle);
        for (idx, handle) in batch.field_handles.into_iter().enumerate() {
            field_handles[idx].push(handle);
        }
        for (idx, tagv) in batch.tag_values.into_iter().enumerate() {
            tag_handles[idx].push(tagv);
        }
    }
    let tsid_array = IndexedArray {
        idx: tsid_kind.idx,
        array: Arc::new(tsid_col),
    };
    let timestamp_array = IndexedArray {
        idx: timestamp_kind.idx,
        array: Arc::new(merge_array_vec_to_list_array(
            timestamp_kind.kind,
            timestamp_handle,
        )?),
    };
    let tag_arrays = tag_handles
        .into_iter()
        .zip(tag_kinds.iter().map(|n| n.idx))
        .map(|(c, idx)| IndexedArray {
            idx,
            array: Arc::new(StringArray::from(c)) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let field_arrays = field_handles
        .into_iter()
        .zip(field_kinds.iter().map(|n| (n.idx, n.kind)))
        .map(|(handle, (idx, datum_kind))| {
            Ok(IndexedArray {
                idx,
                array: Arc::new(merge_array_vec_to_list_array(datum_kind, handle)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let all_columns = vec![vec![tsid_array, timestamp_array], tag_arrays, field_arrays]
        .into_iter()
        .flatten()
        .map(|indexed_array| (indexed_array.idx, indexed_array.array))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();

    ArrowRecordBatch::try_new(arrow_schema, all_columns)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(EncodeRecordBatch)
}

pub fn convert_to_hybrid(
    schema: &Schema,
    hybrid_arrow_schema: ArrowSchemaRef,
    tsid_idx: usize,
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    let timestamp_type = IndexedKind {
        idx: schema.timestamp_index(),
        kind: schema.column(schema.timestamp_index()).data_type,
    };
    let tsid_type = IndexedKind {
        idx: tsid_idx,
        kind: schema.column(tsid_idx).data_type,
    };

    let mut tag_types = Vec::new();
    let mut field_types = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_types.push(IndexedKind {
                idx,
                kind: schema.column(idx).data_type,
            });
        } else if idx != timestamp_type.idx && idx != tsid_type.idx {
            field_types.push(IndexedKind {
                idx,
                kind: schema.column(idx).data_type,
            });
        }
    }
    // TODO: should keep tsid ordering here?
    let mut batch_by_tsid = BTreeMap::new();
    for record_batch in arrow_record_batch_vec {
        let tsid_array = record_batch
            .column(tsid_type.idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked when create table");

        if tsid_array.is_empty() {
            continue;
        }

        let tagv_columns = tag_types
            .iter()
            .map(|indexed_name| {
                record_batch
                    .column(indexed_name.idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("checked when create table")
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

            let mut field_columns = Vec::with_capacity(field_types.len());
            for indexed_type in &field_types {
                let fields_in_one_tsid =
                    record_batch.column(indexed_type.idx).slice(offset, length);
                field_columns.push(fields_in_one_tsid)
            }
            let batch = batch_by_tsid.entry(tsid).or_insert_with(|| {
                TsidBatch::new(
                    tagv_columns
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect(),
                    record_batch.column(timestamp_type.idx).clone(),
                    field_types
                        .iter()
                        .map(|indexed_name| record_batch.column(indexed_name.idx).clone())
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
        tag_types,
        field_types,
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
    fn merge_timestamp_array_list() {
        let list_of_arrays = vec![
            ArrayHandle::with_positions(timestamp_array(1, 20), vec![(1, 2), (10, 3)]),
            ArrayHandle::with_positions(timestamp_array(100, 120), vec![(1, 2), (10, 3)]),
        ];

        let data = vec![
            Some(vec![Some(2), Some(3), Some(11), Some(12), Some(13)]),
            Some(vec![Some(101), Some(102), Some(110), Some(111), Some(112)]),
        ];
        let expected = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(data);
        let list_array =
            merge_array_vec_to_list_array(DatumKind::Timestamp, list_of_arrays).unwrap();

        assert_eq!(list_array, expected);
    }

    #[test]
    fn merge_u16_array_list() {
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
        let list_array = merge_array_vec_to_list_array(DatumKind::UInt16, list_of_arrays).unwrap();

        assert_eq!(list_array, expected);
    }
}
