use std::{collections::BTreeMap, sync::Arc};

use arrow_deps::arrow::{
    array::{Array, ArrayData, ArrayRef, ListArray, StringArray, UInt64Array},
    buffer::MutableBuffer,
    datatypes::Schema as ArrowSchema,
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::{
    datum::DatumKind,
    schema::{ArrowSchemaRef, DataType, Field, Schema},
};
use log::debug;
use snafu::{OptionExt, ResultExt};

use crate::sst::builder::{EncodeRecordBatch, NotSupportedArrowType, Result, VariableLengthType};

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
    fn buffer_slice(&self) -> &[u8] {
        self.array.data().buffers()[0].as_slice()
    }
}

/// `TsidBatch` is used to collect column data for the same TSID
/// timestamps.len == each field len
/// NOTE: only support f64 fields now
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
struct IndexedType {
    idx: usize,
    arrow_type: DataType,
}

struct IndexedArray {
    idx: usize,
    array: ArrayRef,
}

pub fn build_hybrid_arrow_schema(schema: &Schema) -> ArrowSchemaRef {
    let tsid_idx = schema.index_of_tsid();
    if tsid_idx.is_none() {
        return schema.to_arrow_schema_ref();
    };

    let tsid_idx = tsid_idx.unwrap();
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

fn merge_array_vec_to_list(
    data_type: DataType,
    list_of_arrays: Vec<ArrayHandle>,
) -> Result<ListArray> {
    assert!(!list_of_arrays.is_empty());

    let array_len = list_of_arrays.len();
    let datum_kind = DatumKind::from_data_type(&data_type).context(NotSupportedArrowType {
        type_str: data_type.to_string(),
    })?;
    let data_type_size = datum_kind.size().context(VariableLengthType {
        type_str: datum_kind.to_string(),
    })?;

    let total_value_num = list_of_arrays.iter().map(|handle| handle.len()).sum();
    let value_total_bytes = total_value_num * data_type_size;
    let mut values = MutableBuffer::new(value_total_bytes);
    let mut offsets = MutableBuffer::new(list_of_arrays.len() * std::mem::size_of::<i32>());
    let mut length_so_far: i32 = 0;
    offsets.push(length_so_far);

    for array_handle in list_of_arrays {
        let shared_buffer = array_handle.buffer_slice();
        for (offset, length) in &array_handle.positions {
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
        .len(total_value_num)
        .add_buffer(values.into())
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
    tsid_type: IndexedType,
    timestamp_type: IndexedType,
    tag_types: Vec<IndexedType>,
    field_types: Vec<IndexedType>,
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_col = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned());
    let mut timestamp_handle = Vec::new();
    let mut field_handles = vec![Vec::new(); field_types.len()];
    let mut tag_handles = vec![Vec::new(); tag_types.len()];

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
        idx: tsid_type.idx,
        array: Arc::new(tsid_col),
    };
    let timestamp_array = IndexedArray {
        idx: timestamp_type.idx,
        array: Arc::new(merge_array_vec_to_list(
            timestamp_type.arrow_type,
            timestamp_handle,
        )?),
    };
    let tag_arrays = tag_handles
        .into_iter()
        .zip(tag_types.iter().map(|n| n.idx))
        .map(|(c, idx)| IndexedArray {
            idx,
            array: Arc::new(StringArray::from(c)) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let field_arrays = field_handles
        .into_iter()
        .zip(field_types.iter().map(|n| (n.idx, n.arrow_type.clone())))
        .map(|(handle, (idx, arrow_type))| {
            Ok(IndexedArray {
                idx,
                array: Arc::new(merge_array_vec_to_list(arrow_type, handle)?),
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
    arrow_schema: ArrowSchemaRef,
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_idx = schema.index_of_tsid();
    if tsid_idx.is_none() {
        // TODO: check this when create table
        // if table has no tsid, then return back directly.
        return ArrowRecordBatch::concat(&arrow_schema, &arrow_record_batch_vec)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch);
    }

    let timestamp_type = IndexedType {
        idx: schema.timestamp_index(),
        arrow_type: arrow_schema
            .field(schema.timestamp_index())
            .data_type()
            .clone(),
    };
    let tsid_type = IndexedType {
        idx: tsid_idx.unwrap(),
        arrow_type: arrow_schema.field(tsid_idx.unwrap()).data_type().clone(),
    };

    let mut tag_types = Vec::new();
    let mut field_types = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_types.push(IndexedType {
                idx,
                arrow_type: arrow_schema.field(idx).data_type().clone(),
            });
        } else if idx != timestamp_type.idx && idx != tsid_type.idx {
            field_types.push(IndexedType {
                idx,
                arrow_type: arrow_schema.field(idx).data_type().clone(),
            });
        }
    }
    debug!(
        "tsid:{:?}, ts:{:?}, tags:{:?}, fields:{:?}",
        tsid_type, timestamp_type, tag_types, field_types
    );
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
        arrow_schema,
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
        datatypes::{TimeUnit, TimestampMillisecondType},
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
            Some(vec![Some(1), Some(2), Some(10), Some(11), Some(12)]),
            Some(vec![Some(100), Some(101), Some(110), Some(111), Some(112)]),
        ];
        let expected = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(data);
        let list_array = merge_array_vec_to_list(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            list_of_arrays,
        )
        .unwrap();

        // TODO: null bitmaps is not equals now
        assert_eq!(list_array.data().buffers(), expected.data().buffers());
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
        let expected = ListArray::from_iter_primitive::<TimestampMillisecondType, _, _>(data);
        let list_array = merge_array_vec_to_list(DataType::UInt16, list_of_arrays).unwrap();

        // TODO: null bitmaps is not equals now
        assert_eq!(list_array.data().buffers(), expected.data().buffers());
    }
}
