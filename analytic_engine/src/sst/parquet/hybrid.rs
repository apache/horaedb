use std::{collections::BTreeMap, sync::Arc};

use arrow_deps::arrow::{
    array::{
        Array, ArrayRef, Float64Array, ListArray, StringArray, TimestampMillisecondArray,
        UInt64Array,
    },
    datatypes::{Float64Type, Schema as ArrowSchema, TimeUnit, TimestampMillisecondType},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::schema::{DataType, Field, Schema};
use log::debug;
use snafu::ResultExt;

use crate::sst::builder::{EncodeRecordBatch, Result};

//  hard coded in https://github.com/apache/arrow-rs/blob/20.0.0/arrow/src/array/array_list.rs#L185
const LIST_ITEM_NAME: &str = "item";

struct RecordWrapper {
    timestamps: Vec<Option<i64>>,
    tag_values: Vec<String>,
    fields: Vec<Vec<Option<f64>>>,
}

impl RecordWrapper {
    fn new(tag_values: Vec<String>, field_num: usize) -> Self {
        Self {
            timestamps: Vec::new(),
            tag_values,
            fields: Vec::with_capacity(field_num),
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

fn build_hybrid_record(
    schema: Schema,
    tsid_name: IndexedName,
    timestamp_name: IndexedName,
    tag_names: Vec<IndexedName>,
    field_names: Vec<IndexedName>,
    records_by_tsid: BTreeMap<u64, RecordWrapper>,
) -> Result<ArrowRecordBatch> {
    let tsid_col = UInt64Array::from_iter_values(records_by_tsid.keys().cloned().into_iter());
    let mut ts_col = Vec::new();
    let mut field_cols = vec![Vec::new(); field_names.len()];
    let mut tag_cols = vec![Vec::new(); tag_names.len()];

    for record_wrapper in records_by_tsid.into_values() {
        ts_col.push(Some(record_wrapper.timestamps.clone()));
        for (idx, field) in record_wrapper.fields.into_iter().enumerate() {
            field_cols[idx].push(Some(field));
        }
        for (idx, tagv) in record_wrapper.tag_values.into_iter().enumerate() {
            tag_cols[idx].push(tagv);
        }
    }

    let tsid_field = IndexedField {
        idx: tsid_name.idx,
        field: Field::new(&tsid_name.name, DataType::UInt64, false),
    };
    let timestamp_field = IndexedField {
        idx: timestamp_name.idx,
        field: Field::new(
            "timestamp",
            DataType::List(Box::new(Field::new(
                LIST_ITEM_NAME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ))),
            false,
        ),
    };
    let tag_fields = tag_names
        .iter()
        .map(|n| IndexedField {
            idx: n.idx,
            field: Field::new(&n.name, DataType::Utf8, true),
        })
        .collect::<Vec<_>>();
    let field_fields = field_names
        .iter()
        .map(|n| IndexedField {
            idx: n.idx,
            field: Field::new(
                &n.name,
                DataType::List(Box::new(Field::new(
                    LIST_ITEM_NAME,
                    DataType::Float64,
                    true,
                ))),
                true,
            ),
        })
        .collect::<Vec<_>>();

    let all_fields = vec![vec![timestamp_field, tsid_field], tag_fields, field_fields]
        .into_iter()
        .flatten()
        .map(|indexed_field| (indexed_field.idx, indexed_field.field))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();

    let arrow_schema = ArrowSchema::new_with_metadata(
        all_fields,
        schema.into_arrow_schema_ref().metadata().clone(),
    );

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

    ArrowRecordBatch::try_new(Arc::new(arrow_schema), all_columns)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(EncodeRecordBatch)
}

/// Schema should match RecordBatch
fn convert_to_hybrid(
    schema: Schema,
    arrow_record_batch_vec: Vec<ArrowRecordBatch>,
) -> Result<ArrowRecordBatch> {
    let tsid_idx = schema.index_of_tsid();
    if tsid_idx.is_none() {
        // if table has no tsid, then return back directly.
        return ArrowRecordBatch::concat(&schema.into_arrow_schema_ref(), &arrow_record_batch_vec)
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
    let mut records_by_tsid = BTreeMap::new();
    for record_batch in arrow_record_batch_vec {
        let tsid_array = record_batch
            .column(tsid_name.idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked in build plan");

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
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut previous_tsid = tsid_array.value(0);
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
            let record_wrapper = records_by_tsid.entry(tsid).or_insert_with(|| {
                RecordWrapper::new(
                    tagv_columns
                        .iter()
                        .map(|col| col.value(offset).to_string())
                        .collect::<Vec<_>>(),
                    field_names.len(),
                )
            });
            record_wrapper.append_timestamp(timestamps_in_one_tsid.into_iter());
            record_wrapper.append_fields(field_columns);
        }
    }

    build_hybrid_record(
        schema,
        tsid_name,
        timestamp_name,
        tag_names,
        field_names,
        records_by_tsid,
    )
}
