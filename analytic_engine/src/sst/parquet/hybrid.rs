use std::{collections::BTreeMap, sync::Arc};

use arrow_deps::arrow::{
    array::{
        Array, ArrayRef, Float64Array, ListArray, StringArray, TimestampMillisecondArray,
        UInt64Array,
    },
    datatypes::{Float64Type, Schema as ArrowSchema, TimeUnit, TimestampMillisecondType},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::schema::{ArrowSchemaRef, DataType, Field, Schema};
use log::debug;
use snafu::ResultExt;

use crate::sst::builder::{EncodeRecordBatch, Result};

//  hard coded in https://github.com/apache/arrow-rs/blob/20.0.0/arrow/src/array/array_list.rs#L185
const LIST_ITEM_NAME: &str = "item";

/// `TsidBatch` is used to collect column data for the same TSID
/// timestamps.len == each field len
/// NOTE: only support f64 fields now
struct TsidBatch {
    timestamps: Vec<Option<i64>>,
    tag_values: Vec<String>,
    fields: Vec<Vec<Option<f64>>>,
}

impl TsidBatch {
    fn new(tag_values: Vec<String>, field_num: usize) -> Self {
        Self {
            timestamps: Vec::new(),
            tag_values,
            fields: vec![Vec::new(); field_num],
        }
    }

    fn append_timestamp(&mut self, ts: impl IntoIterator<Item = Option<i64>>) {
        self.timestamps.extend(ts);
    }

    fn append_fields(&mut self, fields: Vec<ArrayRef>) {
        assert_eq!(self.fields.len(), fields.len());

        for (idx, fields) in fields.into_iter().enumerate() {
            let fields_in_one_tsid = fields
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("checked in plan build");

            self.fields[idx].extend(fields_in_one_tsid.into_iter());
        }
    }
}

#[derive(Debug)]
struct IndexedName {
    idx: usize,
    name: String,
}

struct IndexedField {
    idx: usize,
    field: Field,
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

fn build_hybrid_record(
    arrow_schema: ArrowSchemaRef,
    tsid_name: IndexedName,
    timestamp_name: IndexedName,
    tag_names: Vec<IndexedName>,
    field_names: Vec<IndexedName>,
    batch_by_tsid: BTreeMap<u64, TsidBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_col = UInt64Array::from_iter_values(batch_by_tsid.keys().cloned().into_iter());
    let mut ts_col = Vec::new();
    let mut field_cols = vec![Vec::new(); field_names.len()];
    let mut tag_cols = vec![Vec::new(); tag_names.len()];

    for batch in batch_by_tsid.into_values() {
        ts_col.push(Some(batch.timestamps.clone()));
        for (idx, field) in batch.fields.into_iter().enumerate() {
            field_cols[idx].push(Some(field));
        }
        for (idx, tagv) in batch.tag_values.into_iter().enumerate() {
            tag_cols[idx].push(tagv);
        }
    }
    let tsid_array = IndexedArray {
        idx: tsid_name.idx,
        array: Arc::new(tsid_col),
    };
    let ts_array = IndexedArray {
        idx: timestamp_name.idx,
        array: Arc::new(ListArray::from_iter_primitive::<
            TimestampMillisecondType,
            _,
            _,
        >(ts_col)),
    };
    let tag_arrays = tag_cols
        .into_iter()
        .zip(tag_names.iter().map(|n| n.idx))
        .map(|(c, idx)| IndexedArray {
            idx,
            array: Arc::new(StringArray::from(c)) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let field_arrays = field_cols
        .into_iter()
        .zip(field_names.iter().map(|n| n.idx))
        .map(|(field_values, idx)| IndexedArray {
            idx,
            array: Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                field_values,
            )),
        })
        .collect::<Vec<_>>();
    let all_columns = vec![vec![tsid_array, ts_array], tag_arrays, field_arrays]
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

/// Schema should match RecordBatch
pub fn convert_to_hybrid(
    schema: &Schema,
    arrow_schema: ArrowSchemaRef,
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    // let schema = Schema::try_from(arrow_schema)
    //     .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    //     .context(EncodeRecordBatch)?;
    let tsid_idx = schema.index_of_tsid();
    if tsid_idx.is_none() {
        // if table has no tsid, then return back directly.
        return ArrowRecordBatch::concat(&arrow_schema, &arrow_record_batch_vec)
            .map_err(|e| Box::new(e) as _)
            .context(EncodeRecordBatch);
    }

    let timestamp_name = IndexedName {
        idx: schema.timestamp_index(),
        name: schema.column(schema.timestamp_index()).name.clone(),
    };
    let tsid_name = IndexedName {
        idx: tsid_idx.unwrap(),
        name: schema.column(tsid_idx.unwrap()).name.clone(),
    };

    let mut tag_names = Vec::new();
    let mut field_names = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_names.push(IndexedName {
                idx,
                name: col.name.clone(),
            });
        } else {
            if idx != timestamp_name.idx && idx != tsid_name.idx {
                field_names.push(IndexedName {
                    idx,
                    name: col.name.clone(),
                });
            }
        }
    }
    debug!(
        "tsid:{:?}, ts:{:?}, tags:{:?}, fields:{:?}",
        tsid_name, timestamp_name, tag_names, field_names
    );
    // TODO: should keep tsid ordering here?
    let mut batch_by_tsid = BTreeMap::new();
    for record_batch in arrow_record_batch_vec {
        let tsid_array = record_batch
            .column(tsid_name.idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked when create table");

        if tsid_array.is_empty() {
            continue;
        }

        let tagv_columns = tag_names
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
        // the length of each tsid occupied can be calculated with
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
            // collect timestamps
            let timestamps_in_one_tsid = record_batch
                .column(timestamp_name.idx)
                .slice(offset, length);
            let timestamps_in_one_tsid = timestamps_in_one_tsid
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("checked in plan build");

            // collect fields
            let mut field_columns = Vec::with_capacity(field_names.len());
            for indexed_name in &field_names {
                let fields_in_one_tsid =
                    record_batch.column(indexed_name.idx).slice(offset, length);
                field_columns.push(fields_in_one_tsid)
            }
            let batch = batch_by_tsid.entry(tsid).or_insert_with(|| {
                TsidBatch::new(
                    tagv_columns
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect::<Vec<_>>(),
                    field_names.len(),
                )
            });
            batch.append_timestamp(timestamps_in_one_tsid.into_iter());
            batch.append_fields(field_columns);
        }
    }

    build_hybrid_record(
        arrow_schema,
        tsid_name,
        timestamp_name,
        tag_names,
        field_names,
        batch_by_tsid,
    )
}
