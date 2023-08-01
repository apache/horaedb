// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! A cli to query sst meta data

use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use analytic_engine::sst::{meta_data::cache::MetaData, parquet::async_reader::ChunkReaderAdapter};
use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use object_store::{LocalFileSystem, ObjectMeta, ObjectStoreRef, Path};
use parquet_ext::{meta_data::fetch_parquet_metadata, reader::ObjectStoreReader};
use runtime::Runtime;
use time_ext::format_as_ymdhms;
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

    /// Print page indexes
    #[clap(short, long, required(false))]
    page_indexes: bool,

    /// Which field to sort ssts[valid: seq/time/size/row].
    #[clap(short, long, default_value = "time")]
    sort: SortBy,
}

#[derive(Debug)]
enum SortBy {
    /// Max Sequence number
    Seq,
    /// Time range
    Time,
    /// File size
    Size,
    /// Row numbers
    Row,
}

impl fmt::Display for SortBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for SortBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sort_by = match s {
            "seq" => Self::Seq,
            "time" => Self::Time,
            "size" => Self::Size,
            "row" => Self::Row,
            _ => return Err(format!("Invalid sort by, value:{s}")),
        };

        Ok(sort_by)
    }
}

#[derive(Default, Debug)]
struct FileStatistics {
    file_count: u64,
    size: usize,
    metadata_size: usize,
    kv_size: usize,
    filter_size: usize,
    row_num: i64,
}

impl ToString for FileStatistics {
    fn to_string(&self) -> String {
        format!("FileStatistics {{\n\tfile_count: {},\n\tsize: {:.2},\n\tmetadata_size: {:.2}, \n\tkv_size: {:.2},\n\tfilter_size: {:.2},\n\trow_num: {},\n}}",
                self.file_count,
                as_mb(self.size),
                as_mb(self.metadata_size),
                as_mb(self.kv_size),
                as_mb(self.filter_size),
                self.row_num)
    }
}

#[derive(Default, Debug)]
struct FieldStatistics {
    compressed_size: i64,
    uncompressed_size: i64,
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
    let page_indexes = args.page_indexes;
    while let Some(object_meta) = ssts.next().await {
        let object_meta = object_meta?;
        let storage = storage.clone();
        let location = object_meta.location.clone();
        join_set.spawn_on(
            async move {
                let (metadata, metadata_size, kv_size) =
                    parse_metadata(storage, location, object_meta.size, verbose, page_indexes)
                        .await?;
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

    match args.sort {
        SortBy::Time => metas.sort_by(|a, b| {
            a.1.custom()
                .time_range
                .inclusive_start()
                .cmp(&b.1.custom().time_range.inclusive_start())
        }),
        SortBy::Seq => {
            metas.sort_by(|a, b| a.1.custom().max_sequence.cmp(&b.1.custom().max_sequence))
        }
        SortBy::Size => metas.sort_by(|a, b| a.0.size.cmp(&b.0.size)),
        SortBy::Row => metas.sort_by(|a, b| {
            a.1.parquet()
                .file_metadata()
                .num_rows()
                .cmp(&b.1.parquet().file_metadata().num_rows())
        }),
    };

    let mut file_stats = FileStatistics::default();
    let mut field_stats_map = HashMap::new();
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

        file_stats.file_count += 1;
        file_stats.size += object_meta.size;
        file_stats.metadata_size += metadata_size;
        file_stats.kv_size += kv_size;
        file_stats.filter_size += filter_size;
        file_stats.row_num += row_num;

        let fields = file_metadata.schema().get_fields();
        for (_, row_group) in parquet_meta.row_groups().iter().enumerate() {
            for i in 0..fields.len() {
                let column_meta = row_group.column(i);
                let field_name = fields.get(i).unwrap().get_basic_info().name().to_string();
                let mut field_stats = field_stats_map
                    .entry(field_name)
                    .or_insert(FieldStatistics::default());
                field_stats.compressed_size += column_meta.compressed_size();
                field_stats.uncompressed_size += column_meta.uncompressed_size();
            }
        }

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

    println!("{}", file_stats.to_string());
    println!("FieldStatistics: ");
    for (k, v) in field_stats_map.iter() {
        println!(
            "{},\t compressed_size: {:.2}mb,\t uncompressed_size: {:.2}mb,\t compress_ratio: {:.2}",
            k,
            as_mb(v.compressed_size as usize),
            as_mb(v.uncompressed_size as usize),
            v.uncompressed_size as f64 / v.compressed_size as f64
        );
    }
    Ok(())
}

fn as_mb(v: usize) -> f64 {
    v as f64 / 1024.0 / 1024.0
}

async fn parse_metadata(
    storage: ObjectStoreRef,
    path: Path,
    size: usize,
    verbose: bool,
    page_indexes: bool,
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

    let md = if page_indexes {
        let object_store_reader =
            ObjectStoreReader::new(storage, path.clone(), Arc::new(parquet_metadata));
        let parquet_metadata =
            parquet_ext::meta_data::meta_with_page_indexes(object_store_reader).await?;
        MetaData::try_new(&parquet_metadata, false, None)?
    } else {
        MetaData::try_new(&parquet_metadata, false, None)?
    };

    Ok((md, metadata_size, kv_size))
}
