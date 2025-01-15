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

pub struct MetricId(u64);
pub struct SeriesId(u64);

pub struct Label {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

/// This is the main struct used for write, optional values will be filled in
/// different modules.
pub struct Sample {
    pub name: Vec<u8>,
    pub lables: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
    /// hash of name
    pub name_id: Option<MetricId>,
    /// hash of labels(sorted)
    pub series_id: Option<SeriesId>,
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}
