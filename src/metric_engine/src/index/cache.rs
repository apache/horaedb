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

use std::collections::{HashMap, HashSet};

use crate::types::{
    FieldName, FieldType, MetricId, MetricName, SegmentDuration, SeriesId, SeriesKey, TagName,
    TagValue,
};

type MetricsCache = HashMap<SegmentDuration, HashMap<MetricName, (FieldName, FieldType)>>;
type SeriesCache = HashMap<SegmentDuration, HashMap<SeriesId, SeriesKey>>;
type TagIndexCache =
    HashMap<SegmentDuration, HashMap<TagName, HashMap<TagValue, HashSet<(SeriesId, MetricId)>>>>;

#[derive(Default)]
pub struct CacheManager {
    metrics: MetricsCache,
    series: SeriesCache,
    tag_index: TagIndexCache,
}

impl CacheManager {
    pub fn update_metric() -> Option<()> {
        todo!()
    }

    pub fn update_series() -> Option<()> {
        todo!()
    }

    pub fn update_tag_index() -> Option<()> {
        todo!()
    }
}
