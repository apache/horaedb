// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Engine open test.

use crate::tests::util::{
    EngineBuildContext, MemoryEngineBuildContext, RocksDBEngineContext, TestEnv,
};

#[test]
fn test_open_engine_rocks() {
    let rocksdb_ctx = RocksDBEngineContext::default();
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
