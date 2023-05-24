// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to query sst meta data

use std::sync::Arc;

use analytic_engine::sst::{
    meta_data::cache::MetaData,
    parquet::{async_reader::ChunkReaderAdapter, meta_data::ParquetMetaDataRef},
};
use anyhow::Result;
use clap::Parser;
use common_util::{
    runtime::{self, Runtime},
    time::format_as_ymdhms,
};
use futures::StreamExt;
use object_store::{LocalFileSystem, ObjectMeta, ObjectStoreRef, Path};
use parquet_ext::meta_data::fetch_parquet_metadata;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Root dir of storage
    #[clap(short, long, required(true))]
    store_path: String,

    /// Sst directory(relative to store_path)
    #[clap(short, long, required(true))]
    dir: String,
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
    let rt = Arc::new(new_runtime(2));
    rt.block_on(async move {
        if let Err(e) = run(args).await {
            eprintln!("Run failed, err:{e}");
        }
    });
}

async fn run(args: Args) -> Result<()> {
    let storage = LocalFileSystem::new_with_prefix(args.store_path)?;
    let storage: ObjectStoreRef = Arc::new(storage);
    let prefix_path = Path::parse(args.dir)?;
    let mut ssts = storage.list(Some(&prefix_path)).await?;

    let mut metas = Vec::new();
    while let Some(object_meta) = ssts.next().await {
        let object_meta = object_meta?;
        let md = parse_metadata(&storage, &object_meta.location, object_meta.size).await?;
        metas.push((object_meta, md));
    }

    // sort by time_range asc
    metas.sort_unstable_by(|a, b| {
        a.1.time_range
            .inclusive_start()
            .cmp(&b.1.time_range.inclusive_start())
    });

    for (object_meta, md) in metas {
        let ObjectMeta { location, size, .. } = object_meta;
        let time_range = md.time_range;
        let start = format_as_ymdhms(time_range.inclusive_start().as_i64());
        let end = format_as_ymdhms(time_range.exclusive_end().as_i64());
        let seq = md.max_sequence;
        println!(
            "Location:{location}, time_range:[{start}, {end}), size:{size},
    max_seq:{seq}"
        );
    }

    Ok(())
}

async fn parse_metadata(
    storage: &ObjectStoreRef,
    path: &Path,
    size: usize,
) -> Result<ParquetMetaDataRef> {
    let reader = ChunkReaderAdapter::new(path, storage);
    let parquet_meta_data = fetch_parquet_metadata(size, &reader).await?;

    let md = MetaData::try_new(&parquet_meta_data, true)?;
    let custom_meta = md.custom();
    Ok(custom_meta.clone())
}
