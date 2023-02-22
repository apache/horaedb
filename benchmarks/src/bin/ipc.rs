// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Instant};

use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_ext::ipc;
use common_util::time::InstantExt;

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
        let compress_opts = ipc::CompressOptions {
            method: ipc::CompressionMethod::Zstd,
            compress_min_length: 0,
        };

        let begin = Instant::now();
        let output =
            ipc::encode_record_batch(&batch.into_arrow_record_batch(), compress_opts).unwrap();
        println!("Arrow IPC encoded size:{}", output.payload.len());

        let mut decode_batches = ipc::decode_record_batches(output.payload, output.method).unwrap();
        let _ = common_types::record_batch::RecordBatch::try_from(decode_batches.swap_remove(0))
            .unwrap();

        let cost = begin.saturating_elapsed();
        println!("Arrow IPC encode/decode cost:{}ms", cost.as_millis());
    }
}
