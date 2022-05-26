// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Arrow 2 bench.

use std::{fs::File, io::BufReader, path::Path, sync::Arc, time::Instant};

use arrow2::io::parquet::read;
use common_util::runtime::Runtime;
use log::info;

use crate::{config::SstBenchConfig, util};

pub struct Arrow2Bench {
    store_path: String,
    pub sst_file_name: String,
    max_projections: usize,
    projection: Vec<usize>,
    runtime: Arc<Runtime>,
}

impl Arrow2Bench {
    pub fn new(config: SstBenchConfig) -> Self {
        let runtime = util::new_runtime(config.runtime_thread_num);

        Arrow2Bench {
            store_path: config.store_path,
            sst_file_name: config.sst_file_name,
            max_projections: config.max_projections,
            projection: Vec::new(),
            runtime: Arc::new(runtime),
        }
    }

    pub fn num_benches(&self) -> usize {
        // One test reads all columns and `max_projections` tests read with projection.
        1 + self.max_projections
    }

    pub fn init_for_bench(&mut self, i: usize) {
        let projection = if i < self.max_projections {
            (0..i + 1).into_iter().collect()
        } else {
            Vec::new()
        };

        self.projection = projection;
    }

    pub fn run_bench(&self) {
        let sst_path = Path::new(&self.store_path).join(&self.sst_file_name);

        self.runtime.block_on(async {
            let open_instant = Instant::now();
            let file = BufReader::new(File::open(sst_path).unwrap());

            let record_reader = if self.projection.is_empty() {
                read::RecordReader::try_new(file, None, None, None, None).unwrap()
            } else {
                read::RecordReader::try_new(file, Some(self.projection.clone()), None, None, None).unwrap()
            };
            let open_cost = open_instant.elapsed();

            let iter_begin_instant = Instant::now();
            let mut total_rows = 0;
            let mut batch_num = 0;
            for record_batch in record_reader {
                let num_rows = record_batch.unwrap().num_rows();
                total_rows += num_rows;
                batch_num += 1;
            }

            info!(
                "\nParquetBench total rows of sst: {}, total batch num: {}, open cost: {:?}, iter cost: {:?}",
                total_rows,
                batch_num,
                open_cost,
                iter_begin_instant.elapsed(),
            );
        });
    }
}
