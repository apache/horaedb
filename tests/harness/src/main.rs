// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#![feature(try_blocks)]

use std::ops::Deref;

use anyhow::Result;
use runner::Runner;
use setup::Environment;

mod case;
mod runner;
mod setup;

/// A guard used for auto server stopping for environment.
struct EnvGuard(Environment);

impl Drop for EnvGuard {
    fn drop(&mut self) {
        self.0.stop_server()
    }
}

impl Deref for EnvGuard {
    type Target = Environment;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let env = EnvGuard(Environment::start_server());
    let client = env.build_client();
    let cases = env.get_case_path();
    let runner = Runner::new(cases, client);

    runner.run().await?;

    Ok(())
}
