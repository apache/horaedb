// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Instant};

use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_ext::ipc;
use common_util::{avro, time::InstantExt};

fn create_batch(rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]);

    let a = Int32Array::from_iter_values(0..rows as i32);
    let b = StringArray::from_iter_values((0..rows).map(|i| i.to_string()));

    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
}

pub fn main() {
    let arrow_batch = create_batch(102400);

    // Arrow IPC
    {
        let batch = common_types::record_batch::RecordBatch::try_from(arrow_batch.clone()).unwrap();
        let compression = ipc::Compression::Zstd;

        let begin = Instant::now();
        let bytes =
            ipc::encode_record_batch(&batch.into_arrow_record_batch(), compression).unwrap();
        println!("Arrow IPC encoded size:{}", bytes.len());

        let decode_batch = ipc::decode_record_batch(bytes, compression).unwrap();
        let _ = common_types::record_batch::RecordBatch::try_from(decode_batch).unwrap();

        let cost = begin.saturating_elapsed();
        println!("Arrow IPC encode/decode cost:{}ms", cost.as_millis());
    }

    // Avro
    {
        let record_schema =
            common_types::schema::RecordSchema::try_from(arrow_batch.schema()).unwrap();
        let batch = common_types::record_batch::RecordBatch::try_from(arrow_batch).unwrap();

        let begin = Instant::now();
        let bytes = avro::record_batch_to_avro_rows(&batch).unwrap();
        println!(
            "Avro encoded size:{}",
            bytes.iter().map(|row| row.len()).sum::<usize>()
        );

        let _decode_batch = avro::avro_rows_to_record_batch(bytes, record_schema);

        let cost = begin.saturating_elapsed();
        println!("Avro encode/decode cost:{}ms", cost.as_millis());
    }
}
