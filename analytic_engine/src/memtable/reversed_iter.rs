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

use std::iter::Rev;

use common_types::record_batch::RecordBatchWithKey;
use generic_error::BoxError;
use snafu::ResultExt;

use crate::memtable::{IterReverse, Result};

/// Reversed columnar iterator.
// TODO(xikai): Now the implementation is not perfect: read all the entries
//  into a buffer and reverse read it. The memtable should support scan in
// reverse  order naturally.
pub struct ReversedColumnarIterator<I> {
    iter: I,
    reversed_iter: Option<Rev<std::vec::IntoIter<Result<RecordBatchWithKey>>>>,
    num_record_batch: usize,
}

impl<I> ReversedColumnarIterator<I>
where
    I: Iterator<Item = Result<RecordBatchWithKey>>,
{
    pub fn new(iter: I, num_rows: usize, batch_size: usize) -> Self {
        Self {
            iter,
            reversed_iter: None,
            num_record_batch: num_rows / batch_size,
        }
    }

    fn init_if_necessary(&mut self) {
        if self.reversed_iter.is_some() {
            return;
        }

        let mut buf = Vec::with_capacity(self.num_record_batch);
        for item in &mut self.iter {
            buf.push(item);
        }
        self.reversed_iter = Some(buf.into_iter().rev());
    }
}

impl<I> Iterator for ReversedColumnarIterator<I>
where
    I: Iterator<Item = Result<RecordBatchWithKey>>,
{
    type Item = Result<RecordBatchWithKey>;

    fn next(&mut self) -> Option<Self::Item> {
        self.init_if_necessary();
        self.reversed_iter
            .as_mut()
            .unwrap()
            .next()
            .map(|v| match v {
                Ok(mut batch_with_key) => {
                    batch_with_key
                        .reverse_data()
                        .box_err()
                        .context(IterReverse)?;

                    Ok(batch_with_key)
                }
                Err(e) => Err(e),
            })
    }
}
