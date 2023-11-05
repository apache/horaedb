// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{ops::Bound, sync::Arc};

use arena::NoopCollector;
use async_trait::async_trait;
use bytes_ext::ByteVec;
use codec::memcomparable::MemComparable;
use common_types::{
    datum::Datum,
    projected_schema::ProjectedSchema,
    record_batch::RecordBatchWithKey,
    row::Row,
    schema::IndexInWriterSchema,
    tests::{build_row, build_schema},
    time::Timestamp,
};

use super::*;
use crate::{
    memtable::{
        factory::{Factory, Options},
        skiplist::factory::SkiplistMemTableFactory,
    },
    table::data,
};

#[async_trait]
pub trait TestMemtableBuilder {
    fn build(&self, data: &[(KeySequence, Row)]) -> MemTableRef;
}

pub struct TestUtil {
    memtable: MemTableRef,
    data: Vec<(KeySequence, Row)>,
}

impl TestUtil {
    pub fn new<B: TestMemtableBuilder>(builder: B) -> Self {
        let data = Self::generate_data();
        let memtable = builder.build(&data);

        Self { memtable, data }
    }

    pub fn memtable(&self) -> MemTableRef {
        self.memtable.clone()
    }

    fn generate_data() -> Vec<(KeySequence, Row)> {
        vec![
            (
                KeySequence::new(1, 1),
                build_row(b"a", 1, 10.0, "v1", 1000, 1_000_000),
            ),
            (
                KeySequence::new(1, 2),
                build_row(b"b", 2, 10.0, "v2", 2000, 2_000_000),
            ),
            (
                KeySequence::new(1, 3),
                build_row(
                    b"c",
                    3,
                    10.0,
                    "primary_key same with next row",
                    3000,
                    3_000_000,
                ),
            ),
            (
                KeySequence::new(1, 4),
                build_row(b"c", 3, 10.0, "v3", 3000, 3_000_000),
            ),
            (
                KeySequence::new(2, 1),
                build_row(b"d", 4, 10.0, "v4", 4000, 4_000_000),
            ),
            (
                KeySequence::new(2, 1),
                build_row(b"e", 5, 10.0, "v5", 5000, 5_000_000),
            ),
            (
                KeySequence::new(2, 3),
                build_row(b"f", 6, 10.0, "v6", 6000, 6_000_000),
            ),
            (
                KeySequence::new(3, 4),
                build_row(b"g", 7, 10.0, "v7", 7000, 7_000_000),
            ),
        ]
    }
}
