// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A cli to query region segment meta

use std::{
    fs::{self},
    sync::Arc,
};

use clap::Parser;
use runtime::Builder;
use tabled::{Table, Tabled};
use wal::local_storage_impl::segment::{Region, RegionManager, SegmentTableMeta};

#[derive(Parser, Debug)]
#[clap(author, version, about = "A command line tool to read and display WAL (Write-Ahead Log) segment metadata", long_about = None)]
struct Args {
    /// Data directory path
    #[clap(short, long, default_value = "/tmp/horaedb/wal")]
    data_dir: String,

    /// Region id
    #[clap(short = 'r', long, default_value = None)]
    region_id: Option<u64>,

    /// Segment id
    #[clap(short = 's', long, default_value = None)]
    segment_id: Option<u64>,

    /// Table id
    #[clap(long, default_value = None)]
    table_id: Option<u64>,
}

#[derive(Tabled)]
struct SegmentInfo {
    segment_id: u64,
    min_seq: u64,
    max_seq: u64,
    version: u8,
    current_size: usize,
    segment_size: usize,
    number_of_records: usize,
}

#[derive(Tabled)]
struct TableInfo {
    table_id: u64,
    min_seq: u64,
    max_seq: u64,
}

impl SegmentInfo {
    fn load(stm: &SegmentTableMeta) -> Self {
        Self {
            segment_id: stm.id,
            min_seq: stm.min_seq,
            max_seq: stm.max_seq,
            version: stm.version,
            current_size: stm.current_size,
            segment_size: stm.segment_size,
            number_of_records: stm.number_of_records,
        }
    }
}

const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

impl TableInfo {
    fn load(stm: &SegmentTableMeta, table_id: &Option<u64>) -> Vec<TableInfo> {
        let mut datas = Vec::new();
        for (t_id, (min_seq, max_seq)) in stm.tables.iter() {
            if table_id.is_some() && table_id.unwrap() != *t_id {
                continue;
            }
            datas.push(TableInfo {
                table_id: *t_id,
                min_seq: *min_seq,
                max_seq: *max_seq,
            });
        }
        datas
    }
}

fn region_meta_dump(region: Arc<Region>, segment_id: &Option<u64>, table_id: &Option<u64>) {
    let segments = region.meta();
    for stm in segments.iter() {
        if segment_id.is_some() && segment_id.unwrap() != stm.id {
            continue;
        }
        println!("{}", "-".repeat(94));
        let pretty_segment = Table::new([SegmentInfo::load(stm)]);
        println!("{}", pretty_segment);
        let pretty_table = Table::new(TableInfo::load(stm, table_id));
        println!("{}", pretty_table);
    }
}

fn pretty_error_then_exit(err_msg: &str) {
    eprintln!("\x1b[31m{}\x1b[0m", err_msg);
    std::process::exit(1);
}

fn main() {
    let args = Args::parse();
    println!("Data directory: {}", args.data_dir);

    if !std::path::Path::new(&args.data_dir).is_dir() {
        pretty_error_then_exit(
            format!("Error: Data directory '{}' does not exist", &args.data_dir).as_str(),
        );
    }

    let runtime = Arc::new(Builder::default().build().unwrap());
    let region_manager = RegionManager::new(args.data_dir.clone(), 32, SEGMENT_SIZE, runtime);
    let region_manager = match region_manager {
        Ok(v) => v,
        Err(e) => {
            pretty_error_then_exit(format!("Error: {}", e).as_str());
            unreachable!();
        }
    };

    if let Some(region_id) = args.region_id {
        let region = region_manager.get_region(region_id);
        region_meta_dump(region.unwrap(), &args.segment_id, &args.table_id);
    } else {
        for entry in fs::read_dir(&args.data_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_file() {
                continue;
            }

            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                // Parse region id from directory name
                if let Ok(region_id) = dir_name.parse::<u64>() {
                    let region = region_manager.get_region(region_id);
                    region_meta_dump(region.unwrap(), &args.segment_id, &args.table_id);
                }
            }
        }
    }

    region_manager.close_all().unwrap();
}
