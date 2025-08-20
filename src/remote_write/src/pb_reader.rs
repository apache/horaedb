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

use std::io::{Error, ErrorKind, Result};

use bytes::{Buf, Bytes};

use crate::pooled_types::{
    PooledExemplar, PooledLabel, PooledMetricMetadata, PooledMetricType, PooledSample,
    PooledTimeSeries, PooledWriteRequest,
};

const WIRE_TYPE_VARINT: u8 = 0;
const WIRE_TYPE_64BIT: u8 = 1;
const WIRE_TYPE_LENGTH_DELIMITED: u8 = 2;

const FIELD_NUM_TIMESERIES: u32 = 1;
const FIELD_NUM_METADATA: u32 = 3;
const FIELD_NUM_LABELS: u32 = 1;
const FIELD_NUM_SAMPLES: u32 = 2;
const FIELD_NUM_EXEMPLARS: u32 = 3;
const FIELD_NUM_LABEL_NAME: u32 = 1;
const FIELD_NUM_LABEL_VALUE: u32 = 2;
const FIELD_NUM_SAMPLE_VALUE: u32 = 1;
const FIELD_NUM_SAMPLE_TIMESTAMP: u32 = 2;
const FIELD_NUM_EXEMPLAR_LABELS: u32 = 1;
const FIELD_NUM_EXEMPLAR_VALUE: u32 = 2;
const FIELD_NUM_EXEMPLAR_TIMESTAMP: u32 = 3;
const FIELD_NUM_METADATA_TYPE: u32 = 1;
const FIELD_NUM_METADATA_FAMILY_NAME: u32 = 2;
const FIELD_NUM_METADATA_HELP: u32 = 4;
const FIELD_NUM_METADATA_UNIT: u32 = 5;

// Taken from https://github.com/v0y4g3r/prom-write-request-bench/blob/step6/optimize-slice/src/bytes.rs under Apache License 2.0.
#[allow(dead_code)]
#[inline(always)]
unsafe fn copy_to_bytes(data: &mut Bytes, len: usize) -> Bytes {
    if len == data.remaining() {
        std::mem::replace(data, Bytes::new())
    } else {
        let ret = unsafe { split_to_unsafe(data, len) };
        data.advance(len);
        ret
    }
}

// Taken from https://github.com/v0y4g3r/prom-write-request-bench/blob/step6/optimize-slice/src/bytes.rs under Apache License 2.0.
#[allow(dead_code)]
#[inline(always)]
pub unsafe fn split_to_unsafe(buf: &Bytes, end: usize) -> Bytes {
    let len = buf.len();
    assert!(
        end <= len,
        "range end out of bounds: {:?} <= {:?}",
        end,
        len,
    );

    if end == 0 {
        return Bytes::new();
    }

    let ptr = buf.as_ptr();
    // `Bytes::drop` does nothing when it's built via `from_static`.
    use std::slice;
    Bytes::from_static(unsafe { slice::from_raw_parts(ptr, end) })
}

pub struct ProtobufReader {
    data: Bytes,
}

impl ProtobufReader {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    pub fn remaining(&self) -> usize {
        self.data.remaining()
    }

    /// Read a varint from the buffer.
    ///
    /// Similar to [quick-protobuf](https://github.com/tafia/quick-protobuf), unroll the loop in
    /// [the official Go implementation](https://cs.opensource.google/go/go/+/refs/tags/go1.24.5:src/encoding/binary/varint.go;l=68)
    /// for better performance.
    #[inline(always)]
    pub fn read_varint(&mut self) -> Result<u64> {
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        // First byte.
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(b as u64);
        }
        let mut x = (b & 0x7f) as u64;
        // Second byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 7));
        }
        x |= ((b & 0x7f) as u64) << 7;
        // Third byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 14));
        }
        x |= ((b & 0x7f) as u64) << 14;
        // Fourth byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 21));
        }
        x |= ((b & 0x7f) as u64) << 21;
        // Fifth byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 28));
        }
        x |= ((b & 0x7f) as u64) << 28;
        // Sixth byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 35));
        }
        x |= ((b & 0x7f) as u64) << 35;
        // Seventh byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 42));
        }
        x |= ((b & 0x7f) as u64) << 42;
        // Eighth byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 49));
        }
        x |= ((b & 0x7f) as u64) << 49;
        // Ninth byte.
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b < 0x80 {
            return Ok(x | ((b as u64) << 56));
        }
        x |= ((b & 0x7f) as u64) << 56;
        // Tenth byte (final byte, must terminate).
        if !self.data.has_remaining() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for varint",
            ));
        }
        let b = self.data.get_u8();
        if b >= 0x80 {
            return Err(Error::new(ErrorKind::InvalidData, "varint overflow"));
        }
        if b > 1 {
            return Err(Error::new(ErrorKind::InvalidData, "varint overflow"));
        }
        Ok(x | ((b as u64) << 63))
    }

    /// Read a double from the buffer.
    #[inline(always)]
    pub fn read_double(&mut self) -> Result<f64> {
        if self.data.remaining() < 8 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for double",
            ));
        }
        // In Protobuf, double is encoded as 64-bit.
        let bits = self.data.get_u64_le();
        Ok(f64::from_bits(bits))
    }

    /// Read a 64-bit integer from the buffer.
    #[inline(always)]
    pub fn read_int64(&mut self) -> Result<i64> {
        // In Protobuf, int64 is encoded as varint.
        self.read_varint().map(|v| v as i64)
    }

    /// Read a string from the buffer.
    pub fn read_string(&mut self) -> Result<Bytes> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for string",
            ));
        }
        // In Protobuf, string is encoded as length-delimited UTF-8 bytes.
        #[cfg(feature = "unsafe-split")]
        let bytes = unsafe { copy_to_bytes(&mut self.data, len) };
        #[cfg(not(feature = "unsafe-split"))]
        let bytes = self.data.split_to(len);
        // Leave the responsibility of validating UTF-8 to the caller,
        // which is the practice of both [easyproto](https://github.com/VictoriaMetrics/easyproto)
        // and [prom-write-request-bench](https://github.com/v0y4g3r/prom-write-request-bench).
        Ok(bytes)
    }

    /// Read a tag from the buffer.
    #[inline(always)]
    pub fn read_tag(&mut self) -> Result<(u32, u8)> {
        // In Protobuf, tag is encoded as varint.
        // tag = (field_number << 3) | wire_type.
        let tag = self.read_varint()?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        Ok((field_number as u32, wire_type as u8))
    }

    /// Read timeseries from the buffer.
    #[inline(always)]
    pub fn read_timeseries(&mut self, timeseries: &mut PooledTimeSeries) -> Result<()> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for timeseries",
            ));
        }
        let start_remaining = self.data.remaining();
        let end_remaining = start_remaining - len;
        while self.data.remaining() > end_remaining {
            let (field_number, wire_type) = self.read_tag()?;
            match field_number {
                FIELD_NUM_LABELS => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "labels")?;
                    let label_ref = timeseries.labels.push_default();
                    self.read_label(label_ref)?;
                }
                FIELD_NUM_SAMPLES => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "samples")?;
                    let sample_ref = timeseries.samples.push_default();
                    self.read_sample(sample_ref)?;
                }
                FIELD_NUM_EXEMPLARS => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "exemplars")?;
                    let exemplar_ref = timeseries.exemplars.push_default();
                    self.read_exemplar(exemplar_ref)?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected field number in timeseries: {}", field_number),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Read label from the buffer.
    #[inline(always)]
    pub fn read_label(&mut self, label: &mut PooledLabel) -> Result<()> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for label",
            ));
        }
        let start_remaining = self.data.remaining();
        let end_remaining = start_remaining - len;
        while self.data.remaining() > end_remaining {
            let (field_number, wire_type) = self.read_tag()?;
            match field_number {
                FIELD_NUM_LABEL_NAME => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "label name")?;
                    label.name = self.read_string()?;
                }
                FIELD_NUM_LABEL_VALUE => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "label value")?;
                    label.value = self.read_string()?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected field number in label: {}", field_number),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Read sample from the buffer.
    #[inline(always)]
    pub fn read_sample(&mut self, sample: &mut PooledSample) -> Result<()> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for sample",
            ));
        }
        let start_remaining = self.data.remaining();
        let end_remaining = start_remaining - len;
        while self.data.remaining() > end_remaining {
            let (field_number, wire_type) = self.read_tag()?;
            match field_number {
                FIELD_NUM_SAMPLE_VALUE => {
                    validate_wire_type(wire_type, WIRE_TYPE_64BIT, "sample value")?;
                    sample.value = self.read_double()?;
                }
                FIELD_NUM_SAMPLE_TIMESTAMP => {
                    validate_wire_type(wire_type, WIRE_TYPE_VARINT, "sample timestamp")?;
                    sample.timestamp = self.read_int64()?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected field number in sample: {}", field_number),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Read exemplar from the buffer.
    #[inline(always)]
    pub fn read_exemplar(&mut self, exemplar: &mut PooledExemplar) -> Result<()> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for exemplar",
            ));
        }
        let start_remaining = self.data.remaining();
        let end_remaining = start_remaining - len;
        while self.data.remaining() > end_remaining {
            let (field_number, wire_type) = self.read_tag()?;
            match field_number {
                FIELD_NUM_EXEMPLAR_LABELS => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "exemplar labels")?;
                    let label_ref = exemplar.labels.push_default();
                    self.read_label(label_ref)?;
                }
                FIELD_NUM_EXEMPLAR_VALUE => {
                    validate_wire_type(wire_type, WIRE_TYPE_64BIT, "exemplar value")?;
                    exemplar.value = self.read_double()?;
                }
                FIELD_NUM_EXEMPLAR_TIMESTAMP => {
                    validate_wire_type(wire_type, WIRE_TYPE_VARINT, "exemplar timestamp")?;
                    exemplar.timestamp = self.read_int64()?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected field number in exemplar: {}", field_number),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Read metric metadata from the buffer.
    #[inline(always)]
    pub fn read_metric_metadata(&mut self, metadata: &mut PooledMetricMetadata) -> Result<()> {
        let len = self.read_varint()? as usize;
        if self.data.remaining() < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough bytes for metadata",
            ));
        }
        let start_remaining = self.data.remaining();
        let end_remaining = start_remaining - len;
        while self.data.remaining() > end_remaining {
            let (field_number, wire_type) = self.read_tag()?;
            match field_number {
                FIELD_NUM_METADATA_TYPE => {
                    validate_wire_type(wire_type, WIRE_TYPE_VARINT, "metadata type")?;
                    let type_value = self.read_varint()? as i32;
                    metadata.metric_type = match type_value {
                        0 => PooledMetricType::Unknown,
                        1 => PooledMetricType::Counter,
                        2 => PooledMetricType::Gauge,
                        3 => PooledMetricType::Histogram,
                        4 => PooledMetricType::GaugeHistogram,
                        5 => PooledMetricType::Summary,
                        6 => PooledMetricType::Info,
                        7 => PooledMetricType::StateSet,
                        _ => PooledMetricType::Unknown,
                    };
                }
                FIELD_NUM_METADATA_FAMILY_NAME => {
                    validate_wire_type(
                        wire_type,
                        WIRE_TYPE_LENGTH_DELIMITED,
                        "metadata family name",
                    )?;
                    metadata.metric_family_name = self.read_string()?;
                }
                FIELD_NUM_METADATA_HELP => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "metadata help")?;
                    metadata.help = self.read_string()?;
                }
                FIELD_NUM_METADATA_UNIT => {
                    validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "metadata unit")?;
                    metadata.unit = self.read_string()?;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected field number in metadata: {}", field_number),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[inline(always)]
fn validate_wire_type(actual: u8, expected: u8, field_name: &str) -> Result<()> {
    if actual != expected {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "expected wire type {} for {}, but found wire type {}",
                expected, field_name, actual
            ),
        ));
    }
    Ok(())
}

/// Fill a PooledWriteRequest instance with data from the buffer.
pub fn read_write_request(data: Bytes, request: &mut PooledWriteRequest) -> Result<()> {
    let mut reader = ProtobufReader::new(data);
    while reader.remaining() > 0 {
        let (field_number, wire_type) = reader.read_tag()?;
        match field_number {
            FIELD_NUM_TIMESERIES => {
                validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "timeseries")?;
                let timeseries_ref = request.timeseries.push_default();
                reader.read_timeseries(timeseries_ref)?;
            }
            FIELD_NUM_METADATA => {
                validate_wire_type(wire_type, WIRE_TYPE_LENGTH_DELIMITED, "metadata")?;
                let metadata_ref = request.metadata.push_default();
                reader.read_metric_metadata(metadata_ref)?;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected field number: {}", field_number),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint_single_byte() {
        let data = &[0x42];
        let mut reader = ProtobufReader::new(Bytes::copy_from_slice(data));
        assert_eq!(reader.read_varint().unwrap(), 66);
    }

    #[test]
    fn test_read_varint_multi_byte() {
        let data = &[0x96, 0x01];
        let mut reader = ProtobufReader::new(Bytes::copy_from_slice(data));
        assert_eq!(reader.read_varint().unwrap(), 150);
    }

    #[test]
    fn test_read_double() {
        let data = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F];
        let mut reader = ProtobufReader::new(Bytes::copy_from_slice(data));
        assert_eq!(reader.read_double().unwrap(), 1.0);
    }

    #[test]
    fn test_read_string() {
        let data = &[0x05, b'h', b'e', b'l', b'l', b'o'];
        let mut reader = ProtobufReader::new(Bytes::copy_from_slice(data));
        assert_eq!(reader.read_string().unwrap(), "hello");
    }

    #[test]
    fn test_parse_write_request() {
        use pb_types::{Exemplar, Label, MetricMetadata, Sample, TimeSeries, WriteRequest};
        use prost::Message;

        let write_request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "metric_name".to_string(),
                    value: "test_value".to_string(),
                }],
                samples: vec![Sample {
                    value: 42.5,
                    timestamp: 1234567890,
                }],
                exemplars: vec![Exemplar {
                    labels: vec![Label {
                        name: "trace_id".to_string(),
                        value: "abc123".to_string(),
                    }],
                    value: 50.0,
                    timestamp: 1234567891,
                }],
            }],
            metadata: vec![MetricMetadata {
                r#type: 1,
                metric_family_name: "test_metric".to_string(),
                help: "Test metric description".to_string(),
                unit: "bytes".to_string(),
            }],
        };

        let encoded = write_request.encode_to_vec();
        let data = Bytes::from(encoded);
        let mut pooled_request = PooledWriteRequest::default();
        read_write_request(data, &mut pooled_request).unwrap();

        assert_eq!(pooled_request.timeseries.len(), 1);
        let ts = &pooled_request.timeseries[0];
        assert_eq!(ts.labels.len(), 1);
        let label = &ts.labels[0];
        assert_eq!(label.name, "metric_name");
        assert_eq!(label.value, "test_value");
        assert_eq!(ts.samples.len(), 1);
        let sample = &ts.samples[0];
        assert_eq!(sample.value, 42.5);
        assert_eq!(sample.timestamp, 1234567890);
        assert_eq!(ts.exemplars.len(), 1);
        let exemplar = &ts.exemplars[0];
        assert_eq!(exemplar.value, 50.0);
        assert_eq!(exemplar.timestamp, 1234567891);
        assert_eq!(exemplar.labels.len(), 1);
        let exemplar_label = &exemplar.labels[0];
        assert_eq!(exemplar_label.name, "trace_id");
        assert_eq!(exemplar_label.value, "abc123");
        assert_eq!(pooled_request.metadata.len(), 1);
        let metadata = &pooled_request.metadata[0];
        assert_eq!(metadata.metric_type, PooledMetricType::Counter);
        assert_eq!(metadata.metric_family_name, "test_metric");
        assert_eq!(metadata.help, "Test metric description");
        assert_eq!(metadata.unit, "bytes");
    }
}
