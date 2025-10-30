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

//! Pooled parser for Prometheus remote write requests.
//!
//! This crate parses the protobuf `string` type as Rust `Bytes` instances
//! instead of `String` instances to avoid allocation, and it **does not**
//! perform UTF-8 validation when parsing. Therefore, it is up to the caller to
//! decide how to make use of the parsed `Bytes` and whether to apply UTF-8
//! validation.

use anyhow::Result;
use bytes::Bytes;
use object_pool::ReusableOwned;

use crate::{
    pb_reader::read_write_request,
    pooled_types::{WriteRequest, POOL},
    repeated_field::Clear,
};

#[derive(Debug, Clone)]
pub struct PooledParser;

impl PooledParser {
    fn new() -> Self {
        Self
    }

    /// Decode a [`WriteRequest`] from the buffer.
    ///
    /// This method will reuse a [`WriteRequest`] instance from the object
    /// pool. After the returned object is dropped, it will be returned to the
    /// pool.
    pub fn decode(&self, buf: Bytes) -> Result<ReusableOwned<WriteRequest>> {
        let mut pooled_request = POOL.pull_owned(WriteRequest::default);
        pooled_request.clear();
        read_write_request(buf, &mut pooled_request)?;
        Ok(pooled_request)
    }
}

impl Default for PooledParser {
    fn default() -> Self {
        Self::new()
    }
}
