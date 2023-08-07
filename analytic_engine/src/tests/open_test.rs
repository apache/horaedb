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

//! Engine open test.

use crate::tests::util::{
    EngineBuildContext, MemoryEngineBuildContext, RocksDBEngineBuildContext, TestEnv,
};

#[test]
fn test_open_engine_rocks() {
    let rocksdb_ctx = RocksDBEngineBuildContext::default();
    test_open_engine(rocksdb_ctx);
}

#[test]
fn test_open_engine_mem_wal() {
    let memory_ctx = MemoryEngineBuildContext::default();
    test_open_engine(memory_ctx);
}

fn test_open_engine<T: EngineBuildContext>(engine_context: T) {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context(engine_context);

    env.block_on(async {
        test_ctx.open().await;

        // Reopen engine.
        test_ctx.reopen().await;
    });
}
