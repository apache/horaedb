// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to convert ssts between different options

use std::{error::Error, sync::Arc};

use analytic_engine::{
    sst::factory::{
        Factory, FactoryImpl, ObjectStorePickerRef, ReadFrequency, SstBuilderOptions,
        SstReaderOptions,
    },
    table_options::{Compression, StorageFormat, StorageFormatHint},
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
    #[clap(short, long, default_value = "zstd")]
    compression: String,

    /// Row group size of new sst file
    #[clap(short, long, default_value_t = 8192)]
    batch_size: usize,

    /// Storage format(values: columnar/hybrid)
    #[clap(short, long, default_value = "columnar")]
    input_format: String,

    /// Storage format(values: columnar/hybrid)
    #[clap(short, long, default_value = "columnar")]
    output_format: String,
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
    let store = Arc::new(storage) as _;
    let input_path = Path::from(args.input);
    let sst_meta = sst_util::meta_from_sst(&store, &input_path).await;
    let factory = FactoryImpl;
    let reader_opts = SstReaderOptions {
        read_batch_row_num: 8192,
        reverse: false,
        frequency: ReadFrequency::Once,
        projected_schema: ProjectedSchema::no_projection(sst_meta.schema.clone()),
        predicate: Arc::new(Predicate::empty()),
        meta_cache: None,
        runtime,
        background_read_parallelism: 1,
        num_rows_per_row_group: 8192,
    };
    let store_picker: ObjectStorePickerRef = Arc::new(store);
    let input_format = StorageFormat::try_from(args.input_format.as_str())
        .with_context(|| format!("invalid input storage format:{}", args.input_format))?;
    let mut reader = factory
        .new_sst_reader(&reader_opts, &input_path, input_format, &store_picker)
        .expect("no sst reader found");

    let output_format_hint = StorageFormatHint::try_from(args.output_format.as_str())
        .with_context(|| format!("invalid storage format:{}", args.output_format))?;
    let builder_opts = SstBuilderOptions {
        storage_format_hint: output_format_hint,
        num_rows_per_row_group: args.batch_size,
        compression: Compression::parse_from(&args.compression)
            .with_context(|| format!("invalid compression:{}", args.compression))?,
    };
    let output = Path::from(args.output);
    let mut builder = factory
        .new_sst_builder(&builder_opts, &output, &store_picker)
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
