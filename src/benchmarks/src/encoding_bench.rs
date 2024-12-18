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
use metric_engine::{
    manifest::{ManifestUpdate, Snapshot},
    sst::{FileMeta, SstFile},
};

// use pb_types;
// use prost::Message;
use crate::config::EncodingConfig;

pub struct EncodingBench {
    // pb_bytes: Bytes,
    raw_bytes: Bytes,
    to_append: Vec<SstFile>,
}

impl EncodingBench {
    pub fn new(config: EncodingConfig) -> Self {
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

        // let pb_manifest = pb_types::Manifest {
        //     files: sstfiles
        //         .clone()
        //         .into_iter()
        //         .map(|f| f.into())
        //         .collect::<Vec<_>>(),
        // };

        // let mut buf: Vec<u8> = Vec::with_capacity(pb_manifest.encoded_len());
        // let _ = pb_manifest.encode(&mut buf);

        let mut snapshot = Snapshot::try_from(Bytes::new()).unwrap();
        let update = ManifestUpdate {
            to_adds: sstfiles,
            to_deletes: vec![],
        };
        let _ = snapshot.merge_update(update);

        EncodingBench {
            // pb_bytes: Bytes::new(),
            raw_bytes: snapshot.into_bytes().unwrap(),
            to_append: vec![sstfile; config.append_count],
        }
    }

    pub fn pb_encoding_bench(&mut self) {
        // mock do_merge procedure
        // first decode snapshot and then append with delta sstfiles, serialize
        // to bytes at last
        // let mut manifest =
        // pb_types::Manifest::decode(self.pb_bytes.clone()).unwrap();
        // manifest.files.extend(
        //     self.to_append
        //         .clone()
        //         .into_iter()
        //         .map(|e| e.into())
        //         .collect::<Vec<pb_types::SstFile>>(),
        // );
        // let mut buf: Vec<u8> = Vec::with_capacity(manifest.encoded_len());
        // let _ = manifest.encode(&mut buf);
    }

    pub fn raw_bytes_bench(&mut self) {
        // mock do_merge procedure
        // first decode snapshot and then append with delta sstfiles, serialize to bytes
        // at last
        let mut snapshot = Snapshot::try_from(self.raw_bytes.clone()).unwrap();
        let update = ManifestUpdate {
            to_adds: self.to_append.clone(),
            to_deletes: vec![],
        };
        let _ = snapshot.merge_update(update);
        let _ = snapshot.into_bytes();
    }
}
