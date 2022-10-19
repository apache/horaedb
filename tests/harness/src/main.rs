// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#![feature(try_blocks)]

use anyhow::Result;
use runner::Runner;
use setup::Environment;

mod case;
mod runner;
mod setup;

#[tokio::main]
async fn main() -> Result<()> {
    let env = Environment::start_server();
    let client = env.build_client();
    let cases = env.get_case_path();
    let runner = Runner::new(cases, client);
    runner.run().await?;
    env.stop_server();

    Ok(())
}
