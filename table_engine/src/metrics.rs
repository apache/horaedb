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

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram_vec, HistogramVec};

lazy_static! {
    pub static ref QUERY_TIME_RANGE: HistogramVec = register_histogram_vec!(
        "query_time_range",
        "Histogram for query time range((15m,30m,...,7d)",
        &["table"],
        exponential_buckets(900.0, 2.0, 10).unwrap()
    )
    .unwrap();
    pub static ref DURATION_SINCE_QUERY_START_TIME: HistogramVec = register_histogram_vec!(
        "duration_since_query_start_time",
        "Histogram for duration since query start time(15m,30m,...,7d)",
        &["table"],
        exponential_buckets(900.0, 2.0, 10).unwrap()
    )
    .unwrap();
}
