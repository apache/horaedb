#![feature(try_blocks)]

use std::env;

use anyhow::Result;
use sqlness::Runner;

mod client;
mod setup;

const CASE_ROOT_PATH_ENV: &str = "CERESDB_TEST_CASE_PATH";

#[tokio::main]
async fn main() -> Result<()> {
    let case_dir = env::var(CASE_ROOT_PATH_ENV)?;
    let env = setup::CeresDBEnv::start_server();
    let config = sqlness::Config {
        case_dir,
        test_case_extension: String::from("sql"),
        output_result_extension: String::from("output"),
        expect_result_extension: String::from("result"),
        interceptor_prefix: String::from("-- SQLNESS"),
        env_config_file: String::from("config.toml"),
    };
    let runner = Runner::new_with_config(config, env).await?;
    runner.run().await?;

    Ok(())
}
