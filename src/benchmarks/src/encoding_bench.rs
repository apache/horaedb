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

//! encoding bench.

use bytes::Bytes;
use columnar_storage::{
    manifest::Snapshot,
    sst::{FileMeta, SstFile},
};

use crate::config::ManifestConfig;

pub struct EncodingBench {
    raw_bytes: Bytes,
    to_append: Vec<SstFile>,
}

impl EncodingBench {
    pub fn new(config: ManifestConfig) -> Self {
        let sstfile = SstFile::new(
            1,
            FileMeta {
                max_sequence: 1,
                num_rows: 1,
                time_range: (1..2).into(),
                size: 1,
            },
        );
        let sstfiles = vec![sstfile.clone(); config.record_count];
        let mut snapshot = Snapshot::try_from(Bytes::new()).unwrap();
        snapshot.add_records(sstfiles);

        EncodingBench {
            raw_bytes: snapshot.into_bytes().unwrap(),
            to_append: vec![sstfile; config.append_count],
        }
    }

    pub fn raw_bytes_bench(&mut self) {
        // mock do_merge procedure
        // first decode snapshot and then append with delta sstfiles, serialize to bytes
        // at last
        let mut snapshot = Snapshot::try_from(self.raw_bytes.clone()).unwrap();
        snapshot.add_records(self.to_append.clone());
        let _ = snapshot.into_bytes();
    }
}
