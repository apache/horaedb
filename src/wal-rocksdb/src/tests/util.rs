// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use runtime::Runtime;
use wal::tests::util::{TestEnv, WalBuilder};

use crate::manager::RocksImpl;

#[derive(Clone, Default)]
pub struct RocksWalBuilder;

#[async_trait]
impl WalBuilder for RocksWalBuilder {
    type Wal = RocksImpl;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let wal_builder = crate::manager::Builder::new(data_path, runtime);

        Arc::new(
            wal_builder
                .build()
                .expect("should succeed to build rocksimpl wal"),
        )
    }
}

pub type RocksTestEnv = TestEnv<RocksWalBuilder>;
