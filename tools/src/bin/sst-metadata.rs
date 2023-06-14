// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to query sst meta data

use std::sync::Arc;

use analytic_engine::sst::{meta_data::cache::MetaData, parquet::async_reader::ChunkReaderAdapter};
use anyhow::{Context, Result};
use clap::Parser;
use common_util::{
    runtime::{self, Runtime},
    time::format_as_ymdhms,
};
use futures::StreamExt;
use object_store::{LocalFileSystem, ObjectMeta, ObjectStoreRef, Path};
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet_ext::meta_data::fetch_parquet_metadata;
use tokio::{runtime::Handle, task::JoinSet};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// SST directory
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
    let storage = LocalFileSystem::new_with_prefix(&args.dir)?;
    let storage: ObjectStoreRef = Arc::new(storage);

    let mut join_set = JoinSet::new();
    let mut ssts = storage.list(None).await?;
    let verbose = args.verbose;
    while let Some(object_meta) = ssts.next().await {
        let object_meta = object_meta?;
        let storage = storage.clone();
        let location = object_meta.location.clone();
        join_set.spawn_on(
            async move {
                let (metadata, metadata_size, kv_size) =
                    parse_metadata(storage, location, object_meta.size, verbose).await?;
                Ok::<_, anyhow::Error>((object_meta, metadata, metadata_size, kv_size))
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
        a.1.custom()
            .time_range
            .inclusive_start()
            .cmp(&b.1.custom().time_range.inclusive_start())
    });

    for (object_meta, sst_metadata, metadata_size, kv_size) in metas {
        let ObjectMeta { location, size, .. } = &object_meta;
        let custom_meta = sst_metadata.custom();
        let parquet_meta = sst_metadata.parquet();
        let time_range = custom_meta.time_range;
        let start = format_as_ymdhms(time_range.inclusive_start().as_i64());
        let end = format_as_ymdhms(time_range.exclusive_end().as_i64());
        let seq = custom_meta.max_sequence;
        let filter_size = custom_meta
            .parquet_filter
            .as_ref()
            .map(|f| f.size())
            .unwrap_or(0);
        let file_metadata = parquet_meta.file_metadata();
        let row_num = file_metadata.num_rows();
        if verbose {
            println!("object_meta:{object_meta:?}, parquet_meta:{parquet_meta:?}, custom_meta:{custom_meta:?}");
        } else {
            let size_mb = as_mb(*size);
            let metadata_mb = as_mb(metadata_size);
            let filter_mb = as_mb(filter_size);
            let kv_mb = as_mb(kv_size);
            println!(
                "Location:{location}, time_range:[{start}, {end}), max_seq:{seq}, size:{size_mb:.3}M, metadata:{metadata_mb:.3}M, kv:{kv_mb:.3}M, filter:{filter_mb:.3}M, row_num:{row_num}"
            );
        }
    }

    Ok(())
}

fn as_mb(v: usize) -> f64 {
    v as f64 / 1024.0 / 1024.0
}

use parquet::file::reader::FileReader;

async fn parse_metadata(
    storage: ObjectStoreRef,
    path: Path,
    size: usize,
    verbose: bool,
) -> Result<(MetaData, usize, usize)> {
    let reader = ChunkReaderAdapter::new(&path, &storage);
    let (parquet_metadata, metadata_size) = fetch_parquet_metadata(size, &reader).await?;
    let kv_metadata = parquet_metadata.file_metadata().key_value_metadata();
    let kv_size = kv_metadata
        .map(|kvs| {
            kvs.iter()
                .map(|kv| {
                    if verbose {
                        println!(
                            "kv_metadata_size, key:{}, value:{:?}",
                            kv.key,
                            kv.value.as_ref().map(|v| v.len())
                        );
                    }

                    kv.key.as_bytes().len() + kv.value.as_ref().map(|v| v.len()).unwrap_or(0)
                })
                .sum()
        })
        .unwrap_or(0);

    // let md = MetaData::try_new(&parquet_metadata, false)?;
    let get_result = storage.get(&path).await.unwrap();
    let bytes = get_result.bytes().await.unwrap();

    let mt = SerializedFileReader::new_with_options(
        bytes,
        ReadOptionsBuilder::new().with_page_index().build(),
    )?;
    let md1 = mt.metadata();

    let md = MetaData::try_new(md1, false)?;
    Ok((md, metadata_size, kv_size))
}
