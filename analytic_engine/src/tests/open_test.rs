// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Engine open test.

use crate::tests::util::TestEnv;

#[test]
fn test_open_engine() {
    let env = TestEnv::builder().build();
    let mut test_ctx = env.new_context();

    env.block_on(async {
        test_ctx.open().await;

        // Reopen engine.
        test_ctx.reopen().await;
    });
}
