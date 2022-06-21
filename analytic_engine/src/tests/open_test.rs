// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Engine open test.

use crate::{
    setup::{EngineBuilder, MemWalEngineBuilder, RocksEngineBuilder},
    tests::util::TestEnv,
};

#[test]
fn test_open_engine_rocks() {
    test_open_engine::<RocksEngineBuilder>();
}

#[test]
fn test_open_engine_mem_wal() {
    test_open_engine::<MemWalEngineBuilder>();
}

fn test_open_engine<T: EngineBuilder>() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context::<T>();

    env.block_on(async {
        test_ctx.open().await;

        // Reopen engine.
        test_ctx.reopen().await;
    });
}
