// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! WalManager write  bench.

use std::sync::Arc;

use common_util::runtime::Runtime;
use rand::prelude::*;
use table_kv::memory::MemoryImpl;
use wal::{
    log_batch::{LogWriteBatch, LogWriteEntry},
    manager::{WalManager, WriteContext},
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl, WalRuntimes},
};

use crate::{
    config::WalWriteBenchConfig,
    util::{self, WritePayload},
};

pub struct WalWriteBench {
    batch_size: usize,
    value_size: usize,
    runtime: Arc<Runtime>,
}

impl WalWriteBench {
    pub fn new(config: WalWriteBenchConfig) -> Self {
        let runtime = util::new_runtime(1);

        WalWriteBench {
            batch_size: config.batch_size,
            value_size: config.value_size,
            runtime: Arc::new(runtime),
        }
    }

    pub fn build_value_vec(&self) -> Vec<Vec<u8>> {
        let value_size = match self.value_size < 128 {
            true => 128,
            false => self.value_size,
        };

        let mut values = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let value = self.random_value(value_size);
            values.push(value);
        }

        values
    }

    pub fn random_value(&self, size: usize) -> Vec<u8> {
        let mut value = vec![0u8; size - 4];
        let mut rng = rand::thread_rng();
        value.extend_from_slice(rng.next_u32().to_le_bytes().as_slice());
        value
    }

    pub fn run_bench(&self) {
        self.runtime.block_on(async {
            let runtimes = WalRuntimes {
                read_runtime: self.runtime.clone(),
                write_runtime: self.runtime.clone(),
                bg_runtime: self.runtime.clone(),
            };

            let wal = WalNamespaceImpl::open(
                MemoryImpl::default(),
                runtimes.clone(),
                "ceresedb",
                NamespaceConfig::default(),
            )
            .await
            .unwrap();

            let values = self.build_value_vec();
            let payloads = values
                .iter()
                .map(|value| WritePayload(value))
                .collect::<Vec<_>>();

            let mut log_batch = LogWriteBatch::with_capacity(1, values.len());
            for payload in payloads.iter() {
                log_batch.push(LogWriteEntry { payload });
            }

            // Write to wal manager
            let write_ctx = WriteContext::default();
            let _ = wal.write(&write_ctx, &log_batch).await.unwrap();
        });
    }
}
