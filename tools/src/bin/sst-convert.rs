// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to convert ssts between different options

use std::{error::Error, sync::Arc};

use analytic_engine::{
    sst::factory::{Factory, FactoryImpl, SstBuilderOptions, SstReaderOptions, SstType},
    table_options::Compression,
};
use anyhow::{Context, Result};
use clap::Parser;
use common_types::{projected_schema::ProjectedSchema, request_id::RequestId};
use common_util::runtime::{self, Runtime};
use futures::stream::StreamExt;
use object_store::{LocalFileSystem, Path};
use table_engine::predicate::Predicate;
use tools::sst_util;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Root dir of storage
    #[clap(short, long, required(true))]
    store_path: String,

    /// Input sst file(relative to store_path)
    #[clap(short, long, required(true))]
    input: String,

    /// Output sst file(relative to store_path)
    #[clap(short, long, required(true))]
    output: String,

    /// Compression of new sst file(values: uncompressed/lz4/snappy/zstd)
    #[clap(short, long, default_value = "uncompressed")]
    compression: String,

    /// Row group size of new sst file
    #[clap(short, long, default_value_t = 8192)]
    batch_size: usize,
}

fn new_runtime(thread_num: usize) -> Runtime {
    runtime::Builder::default()
        .thread_name("tools")
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap()
}

fn main() {
    let args = Args::parse();
    let rt = Arc::new(new_runtime(4));
    let rt2 = rt.clone();
    rt.block_on(async move {
        if let Err(e) = run(args, rt2).await {
            eprintln!("Convert failed, err:{}", e);
        }
    });
}

async fn run(args: Args, runtime: Arc<Runtime>) -> Result<()> {
    let storage = LocalFileSystem::new_with_prefix(args.store_path).expect("invalid path");
    let storage = Arc::new(storage) as _;
    let input_path = Path::from(args.input);
    let sst_meta = sst_util::meta_from_sst(&storage, &input_path, &None, &None).await;
    let factory = FactoryImpl;
    let reader_opts = SstReaderOptions {
        sst_type: SstType::Parquet,
        read_batch_row_num: 8192,
        reverse: false,
        projected_schema: ProjectedSchema::no_projection(sst_meta.schema.clone()),
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        data_cache: None,
        runtime,
    };
    let mut reader = factory
        .new_sst_reader(&reader_opts, &input_path, &storage)
        .expect("no sst reader found");

    let builder_opts = SstBuilderOptions {
        sst_type: SstType::Parquet,
        num_rows_per_row_group: args.batch_size,
        compression: Compression::parse_from(&args.compression)
            .with_context(|| format!("invalid compression:{}", args.compression))?,
    };
    let output = Path::from(args.output);
    let mut builder = factory
        .new_sst_builder(&builder_opts, &output, &storage)
        .expect("no sst builder found");
    let sst_stream = reader
        .read()
        .await
        .unwrap()
        .map(|batch| batch.map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>));
    let sst_stream = Box::new(sst_stream) as _;
    let sst_info = builder
        .build(RequestId::next_id(), &sst_meta, sst_stream)
        .await?;

    println!("Write success, info:{:?}", sst_info);

    Ok(())
}
