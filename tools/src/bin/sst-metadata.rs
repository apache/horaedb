// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to query sst meta data

use std::sync::Arc;

use analytic_engine::sst::{
    meta_data::cache::MetaData,
    parquet::{async_reader::ChunkReaderAdapter, meta_data::ParquetMetaDataRef},
};
use anyhow::{Context, Result};
use clap::Parser;
use common_util::{
    runtime::{self, Runtime},
    time::format_as_ymdhms,
};
use futures::StreamExt;
use object_store::{LocalFileSystem, ObjectMeta, ObjectStoreRef, Path};
use parquet_ext::meta_data::fetch_parquet_metadata;
use tokio::{runtime::Handle, task::JoinSet};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Root dir of storage
    #[clap(short, long, required(true))]
    store_path: String,

    /// SST directory(relative to store_path)
    #[clap(short, long, required(true))]
    dir: String,

    /// Verbose print
    #[clap(short, long, required(false))]
    verbose: bool,

    /// Thread num, 0 means cpu num
    #[clap(short, long, default_value_t = 0)]
    threads: usize,
}

fn new_runtime(thread_num: usize) -> Runtime {
    runtime::Builder::default()
        .thread_name("sst-metadata")
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap()
}

fn main() {
    let args = Args::parse();
    let thread_num = if args.threads == 0 {
        num_cpus::get()
    } else {
        args.threads
    };
    let rt = Arc::new(new_runtime(thread_num));
    rt.block_on(async move {
        if let Err(e) = run(args).await {
            eprintln!("Run failed, err:{e}");
        }
    });
}

async fn run(args: Args) -> Result<()> {
    let handle = Handle::current();
    let storage = LocalFileSystem::new_with_prefix(args.store_path)?;
    let storage: ObjectStoreRef = Arc::new(storage);
    let prefix_path = Path::parse(args.dir)?;

    let mut join_set = JoinSet::new();
    let mut ssts = storage.list(Some(&prefix_path)).await?;
    while let Some(object_meta) = ssts.next().await {
        let object_meta = object_meta?;
        let storage = storage.clone();
        let location = object_meta.location.clone();
        join_set.spawn_on(
            async move {
                let md = parse_metadata(storage, location, object_meta.size).await?;
                Ok::<_, anyhow::Error>((object_meta, md))
            },
            &handle,
        );
    }

    let mut metas = Vec::with_capacity(join_set.len());
    while let Some(meta) = join_set.join_next().await {
        let meta = meta.context("join err")?;
        let meta = meta.context("parse metadata err")?;
        metas.push(meta);
    }

    // sort by time_range asc
    metas.sort_by(|a, b| {
        a.1.time_range
            .inclusive_start()
            .cmp(&b.1.time_range.inclusive_start())
    });

    for (object_meta, parquet_meta) in metas {
        let ObjectMeta { location, size, .. } = &object_meta;
        let time_range = parquet_meta.time_range;
        let start = format_as_ymdhms(time_range.inclusive_start().as_i64());
        let end = format_as_ymdhms(time_range.exclusive_end().as_i64());
        let seq = parquet_meta.max_sequence;
        if args.verbose {
            println!("object_meta:{object_meta:?}, parquet_meta:{parquet_meta:?}, time_range::[{start}, {end})");
        } else {
            let size_mb = *size as f64 / 1024.0 / 1024.0;
            println!(
                "Location:{location}, time_range:[{start}, {end}), size:{size_mb:.3}M, max_seq:{seq}"
            );
        }
    }

    Ok(())
}

async fn parse_metadata(
    storage: ObjectStoreRef,
    path: Path,
    size: usize,
) -> Result<ParquetMetaDataRef> {
    let reader = ChunkReaderAdapter::new(&path, &storage);
    let parquet_meta_data = fetch_parquet_metadata(size, &reader).await?;

    let md = MetaData::try_new(&parquet_meta_data, true)?;
    let custom_meta = md.custom();
    Ok(custom_meta.clone())
}
