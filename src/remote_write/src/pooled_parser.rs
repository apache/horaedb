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

use crate::{
    pb_reader::read_write_request,
    pooled_types::{WriteRequest, WriteRequestManager, POOL},
    repeated_field::Clear,
};

#[derive(Debug, Clone)]
pub struct PooledParser;

impl PooledParser {
    fn new() -> Self {
        Self
    }

    /// Decode a [`WriteRequest`] from the buffer and return it.
    pub fn decode(&self, buf: Bytes) -> Result<WriteRequest> {
        // Cannot get a WriteRequest instance from the pool in sync functions.
        let mut request = WriteRequest::default();
        read_write_request(buf, &mut request)?;
        Ok(request)
    }

    /// Decode a [`WriteRequest`] from the buffer and return a pooled object.
    ///
    /// This method will reuse a [`WriteRequest`] instance from the object
    /// pool. After the returned object is dropped, it will be returned to the
    pub async fn decode_async(
        &self,
        buf: Bytes,
    ) -> Result<deadpool::managed::Object<WriteRequestManager>> {
        let mut pooled_request = POOL
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object from pool: {e:?}"))?;
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
