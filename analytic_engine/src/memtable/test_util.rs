// Copyright 2023 The HoraeDB Authors
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

use common_types::row::Row;

use super::*;

pub trait TestMemtableBuilder {
    fn build(&self, data: &[(KeySequence, Row)]) -> MemTableRef;
}

pub struct TestUtil {
    memtable: MemTableRef,
    data: Vec<(KeySequence, Row)>,
}

impl TestUtil {
    pub fn new<B: TestMemtableBuilder>(builder: B, data: Vec<(KeySequence, Row)>) -> Self {
        let memtable = builder.build(&data);

        Self { memtable, data }
    }

    pub fn memtable(&self) -> MemTableRef {
        self.memtable.clone()
    }

    pub fn data(&self) -> Vec<Row> {
        self.data.iter().map(|d| d.1.clone()).collect()
    }
}
