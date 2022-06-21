use std::collections::HashMap;
use std::convert::TryFrom;
use std::process::id;
use arrow_deps::arrow::{
    array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{Float64Type, TimestampMillisecondType},
    record_batch::RecordBatch as ArrowRecordBatch,
};
use common_types::schema::Schema;

// tagkey => { tagvalue => tsid }
type IndexMap = HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<TSID>>>;
type TSID = u64;

fn build_index(&mut self,arrow_record_batch_vec: &Vec<ArrowRecordBatch>) {
    let mut index_map: IndexMap = HashMap::new();
    let arrow_schema = arrow_record_batch_vec[0].schema();
    let schema = Schema::try_from(arrow_schema.clone()).unwrap();

    let tsid_idx = schema.index_of_tsid().unwrap();

    let mut tag_idxes = Vec::new();
    for (idx, col) in schema.columns().iter().enumerate() {
        if col.is_tag {
            tag_idxes.push(idx);
            index_map.insert(col.name.into_bytes(), HashMap::new());
        }
    }

    for record_batch in arrow_record_batch_vec {
        let tsid_array = record_batch
            .column(tsid_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("checked in build plan");

        if tsid_array.is_empty() {
            continue;
        }

        let mut tagv_columns = Vec::with_capacity(tag_idxes.len());
        for col_idx in &tag_idxes {
            let v = record_batch
                .column(*col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            tagv_columns.push(v);
            for (i, tagvalue_opt) in v.iter().enumerate() {
                if let Some(tagvalue) = tagvalue_opt {
                    let map = index_map.get_mut(
                        schema.column(col_idx.clone()).name.as_bytes());
                    if let Some(map_inner) = map {
                        map_inner.entry(tagvalue.as_bytes().to_vec()).or_default().push(tsid_array.value(i));
                    }
                }
            }
        }
    }

    index_map
}