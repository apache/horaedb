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
//!
//! ## Basic Usage
//!
//! Synchronous parse:
//!
//! ```rust
//! use bytes::Bytes;
//! use remote_write::pooled_parser::PooledParser;
//! # use std::error::Error;
//! # fn main() -> Result<(), Box<dyn Error>> {
//! # let data = Bytes::new();
//!
//! let parser = PooledParser::default();
//! let request = parser.decode(data.clone())?; // Return a `PooledWriteRequest` instance.
//!
//! # Ok(())
//! # }
//! ```
//!
//! Asynchronous parse:
//!
//! ```rust
//! use bytes::Bytes;
//! use remote_write::pooled_parser::PooledParser;
//! # use std::error::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn Error>> {
//! # let data = Bytes::new();
//! let parser = PooledParser::default();
//! let request = parser.decode_async(data).await?; // Return a deadpool object wrapper.
//!
//! // Access the parsed data through the deadpool object wrapper.
//! println!("Parsed {} timeseries", request.timeseries.len());
//! for ts in &request.timeseries {
//!     for label in &ts.labels {
//!         let name = String::from_utf8_lossy(&label.name);
//!         let value = String::from_utf8_lossy(&label.value);
//!         println!("Label: {}={}", name, value);
//!     }
//! }
//! // Object automatically returned to the pool when dropped.
//! # Ok(())
//! # }
//! ```
//!
//! Note: if you use `decode_async()`, you need to add `deadpool` to your
//! dependencies:
//!
//! ```toml
//! [dependencies]
//! deadpool = { workspace = true }  # or "0.10" if not inside this workspace.
//! ```
//!
//! ## Enable Unsafe Optimization
//!
//! ```toml
//! [dependencies]
//! remote_write = { path = ".", features = ["unsafe-split"] }
//! ```
//!
//! This feature enables zero-copy bytes split that bypasses Rust's memory
//! safety guarantees. The parsed `Bytes` instances **cannot outlive** the
//! input raw protobuf `Bytes` instance.
//!
//! Correct usage example:
//!
//! ```rust,no_run
//! use bytes::Bytes;
//! use remote_write::pooled_parser::PooledParser;
//! # use std::error::Error;
//! // CORRECT: consume immediately.
//! async fn foo() -> Result<Vec<String>, Box<dyn Error>> {
//!     # fn load_protobuf_data() -> Bytes { Bytes::new() }
//!     let raw_data = load_protobuf_data(); // Bytes.
//!     let parser = PooledParser::default();
//!     let request = parser.decode_async(raw_data).await?;
//!
//!     let metric_names: Vec<String> = request
//!         .timeseries
//!         .iter()
//!         .flat_map(|ts| ts.labels.iter())
//!         .filter(|label| label.name.as_ref() == b"__name__")
//!         .filter_map(|label| String::from_utf8(label.value.to_vec()).ok())
//!         .collect();
//!
//!     Ok(metric_names)
//! }
//! ```
//!
//! Wrong usage example:
//!
//! ```rust,ignore
//! // WRONG: returned parsed data outlives the input.
//! fn bar() -> PooledWriteRequest {
//!     let raw_data = load_protobuf_data(); // Bytes
//!     let parser = PooledParser::default();
//!     parser.decode(raw_data).unwrap()
//!     // The input `raw_data` is dropped here, but the returned `PooledWriteRequest` outlives it.
//! }
//! ```

use bytes::Bytes;

use crate::{
    pb_reader::read_write_request,
    pooled_types::{PooledWriteRequest, WriteRequestManager, POOL},
};

#[derive(Debug, Clone)]
pub struct PooledParser;

impl PooledParser {
    fn new() -> Self {
        Self
    }

    /// Decode a [`PooledWriteRequest`] from the buffer and return it.
    ///
    /// This method will allocate a new [`PooledWriteRequest`] instance since it
    /// is unable to use the object pool in sync functions.
    ///
    /// # Example
    ///
    /// ```rust
    /// use bytes::Bytes;
    /// use remote_write::pooled_parser::PooledParser;
    /// # use std::error::Error;
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// # let data = Bytes::new();
    ///
    /// let parser = PooledParser::default();
    /// let request = parser.decode(data)?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn decode(&self, buf: Bytes) -> Result<PooledWriteRequest, String> {
        // Cannot get a PooledWriteRequest instance from the pool in sync functions.
        let mut request = PooledWriteRequest::default();
        read_write_request(buf, &mut request).map_err(|e| e.to_string())?;
        Ok(request)
    }

    /// Decode a [`PooledWriteRequest`] from the buffer and return a
    /// [`deadpool::managed::Object`] wrapper.
    ///
    /// This method will reuse a [`PooledWriteRequest`] instance from the object
    /// pool. After the returned object is dropped, it will be returned to the
    /// pool.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use bytes::Bytes;
    /// use remote_write::pooled_parser::PooledParser;
    /// # use std::error::Error;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn Error>> {
    /// # let data = Bytes::new();
    ///
    /// let parser = PooledParser::default();
    /// let request = parser.decode_async(data).await?;
    ///
    /// // Access the parsed data through the deadpool object wrapper.
    /// println!("Parsed {} timeseries", request.timeseries.len());
    /// for ts in &request.timeseries {
    ///     for label in &ts.labels {
    ///         let name = String::from_utf8_lossy(&label.name);
    ///         let value = String::from_utf8_lossy(&label.value);
    ///         println!("Label: {}={}", name, value);
    ///     }
    /// }
    /// // Object automatically returned to the pool when dropped.
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub async fn decode_async(
        &self,
        buf: Bytes,
    ) -> Result<deadpool::managed::Object<WriteRequestManager>, String> {
        let mut pooled_request = POOL
            .get()
            .await
            .map_err(|e| format!("failed to get object from pool: {:?}", e))?;
        pooled_request.clear();
        read_write_request(buf, &mut pooled_request).map_err(|e| e.to_string())?;
        Ok(pooled_request)
    }
}

impl Default for PooledParser {
    fn default() -> Self {
        Self::new()
    }
}
