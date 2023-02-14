// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Datum holds different kind of data

use std::{convert::TryFrom, fmt, str};

use chrono::{Local, TimeZone};
use serde::ser::{Serialize, Serializer};
use snafu::{Backtrace, ResultExt, Snafu};
use sqlparser::ast::{DataType as SqlDataType, Value};
use ceresdbproto::schema::DataType as DataTypePb;

use crate::{bytes::Bytes, hash::hash64, string::StringBytes, time::Timestamp};
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unsupported SQL data type, type:{}.\nBacktrace:\n{}",
        sql_type,
        backtrace
    ))]
    UnsupportedDataType {
        sql_type: SqlDataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid double or float, err:{}.\nBacktrace:\n{}", source, backtrace))]
    InvalidDouble {
        source: std::num::ParseFloatError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid insert value, kind:{}, value:{:?}.\nBacktrace:\n{}",
        kind,
        value,
        backtrace
    ))]
    InvalidValueType {
        kind: DatumKind,
        value: Value,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid timestamp, err:{}.\nBacktrace:\n{}", source, backtrace))]
    InvalidTimestamp {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid integer, err:{}.\nBacktrace:\n{}", source, backtrace))]
    InvalidInt {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid datum byte, byte:{}.\nBacktrace:\n{}", value, backtrace))]
    InvalidDatumByte { value: u8, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

// FIXME(yingwen): How to handle timezone?

/// Data type of datum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumKind {
    Null = 0,
    Timestamp,
    Double,
    Float,
    Varbinary,
    String,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Int64,
    Int32,
    Int16,
    Int8,
    Boolean,
}

impl DatumKind {
    pub const VALUES: [Self; 15] = [
        Self::Null,
        Self::Timestamp,
        Self::Double,
        Self::Float,
        Self::Varbinary,
        Self::String,
        Self::UInt64,
        Self::UInt32,
        Self::UInt16,
        Self::UInt8,
        Self::Int64,
        Self::Int32,
        Self::Int16,
        Self::Int8,
        Self::Boolean,
    ];

    /// Return true if this is DatumKind::Timestamp
    pub fn is_timestamp(&self) -> bool {
        matches!(self, DatumKind::Timestamp)
    }

    pub fn is_f64_castable(&self) -> bool {
        matches!(
            self,
            Self::Double
                | Self::Float
                | Self::UInt64
                | Self::UInt32
                | Self::UInt16
                | Self::UInt8
                | Self::Int64
                | Self::Int32
                | Self::Int16
                | Self::Int8
        )
    }

    /// Can column of this datum kind used as key column
    pub fn is_key_kind(&self) -> bool {
        matches!(
            self,
            DatumKind::Timestamp
                | DatumKind::Varbinary
                | DatumKind::String
                | DatumKind::UInt64
                | DatumKind::UInt32
                | DatumKind::UInt16
                | DatumKind::UInt8
                | DatumKind::Int64
                | DatumKind::Int32
                | DatumKind::Int16
                | DatumKind::Int8
                | DatumKind::Boolean
        )
    }

    pub fn unsign_kind(&self) -> Option<Self> {
        match self {
            Self::Int64 | Self::UInt64 => Some(Self::UInt64),
            Self::Int32 | Self::UInt32 => Some(Self::UInt32),
            Self::Int16 | Self::UInt16 => Some(Self::UInt16),
            Self::Int8 | Self::UInt8 => Some(Self::UInt8),
            _ => None,
        }
    }

    /// Get name of this kind.
    fn as_str(&self) -> &str {
        match self {
            DatumKind::Null => "null",
            DatumKind::Timestamp => "timestamp",
            DatumKind::Double => "double",
            DatumKind::Float => "float",
            DatumKind::Varbinary => "varbinary",
            DatumKind::String => "string",
            DatumKind::UInt64 => "uint64",
            DatumKind::UInt32 => "uint32",
            DatumKind::UInt16 => "uint16",
            DatumKind::UInt8 => "uint8",
            DatumKind::Int64 => "bigint",
            DatumKind::Int32 => "int",
            DatumKind::Int16 => "smallint",
            DatumKind::Int8 => "tinyint",
            DatumKind::Boolean => "boolean",
        }
    }

    /// Convert into a byte.
    #[inline]
    pub fn into_u8(self) -> u8 {
        self as u8
    }

    /// Return None for variable-length type
    pub fn size(&self) -> Option<usize> {
        let size = match self {
            DatumKind::Null => 0,
            DatumKind::Timestamp => 8,
            DatumKind::Double => 8,
            DatumKind::Float => 4,
            DatumKind::Varbinary => return None,
            DatumKind::String => return None,
            DatumKind::UInt64 => 8,
            DatumKind::UInt32 => 4,
            DatumKind::UInt16 => 2,
            DatumKind::UInt8 => 1,
            DatumKind::Int64 => 8,
            DatumKind::Int32 => 4,
            DatumKind::Int16 => 8,
            DatumKind::Int8 => 8,
            DatumKind::Boolean => 1,
        };
        Some(size)
    }
}

impl TryFrom<&SqlDataType> for DatumKind {
    type Error = Error;

    fn try_from(sql_type: &SqlDataType) -> Result<Self> {
        match sql_type {
            // TODO(yingwen): Consider timezone
            SqlDataType::Timestamp(_, _) => Ok(Self::Timestamp),
            SqlDataType::Real | SqlDataType::Float(_) => Ok(Self::Float),
            SqlDataType::Double => Ok(Self::Double),
            SqlDataType::Boolean => Ok(Self::Boolean),
            SqlDataType::BigInt(_) => Ok(Self::Int64),
            SqlDataType::Int(_) => Ok(Self::Int32),
            SqlDataType::SmallInt(_) => Ok(Self::Int16),
            SqlDataType::String => Ok(Self::String),
            SqlDataType::Custom(objects, _) if objects.0.len() == 1 => {
                match objects.0[0].value.as_str() {
                    "UINT64" | "uint64" => Ok(Self::UInt64),
                    "UINT32" | "uint32" => Ok(Self::UInt32),
                    "UINT16" | "uint16" => Ok(Self::UInt16),
                    "UINT8" | "uint8" => Ok(Self::UInt8),
                    "INT64" | "int64" => Ok(Self::Int64),
                    "INT32" | "int32" => Ok(Self::Int32),
                    "INT16" | "int16" => Ok(Self::Int16),
                    "TINYINT" | "INT8" | "tinyint" | "int8" => Ok(Self::Int8),
                    "VARBINARY" | "varbinary" => Ok(Self::Varbinary),
                    _ => UnsupportedDataType {
                        sql_type: sql_type.clone(),
                    }
                    .fail(),
                }
            }

            // Unlike datafusion, Decimal is not supported now
            _ => UnsupportedDataType {
                sql_type: sql_type.clone(),
            }
            .fail(),
        }
    }
}

impl TryFrom<u8> for DatumKind {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            v if DatumKind::Null.into_u8() == v => Ok(DatumKind::Null),
            v if DatumKind::Timestamp.into_u8() == v => Ok(DatumKind::Timestamp),
            v if DatumKind::Double.into_u8() == v => Ok(DatumKind::Double),
            v if DatumKind::Float.into_u8() == v => Ok(DatumKind::Float),
            v if DatumKind::Varbinary.into_u8() == v => Ok(DatumKind::Varbinary),
            v if DatumKind::String.into_u8() == v => Ok(DatumKind::String),
            v if DatumKind::UInt64.into_u8() == v => Ok(DatumKind::UInt64),
            v if DatumKind::UInt32.into_u8() == v => Ok(DatumKind::UInt32),
            v if DatumKind::UInt16.into_u8() == v => Ok(DatumKind::UInt16),
            v if DatumKind::UInt8.into_u8() == v => Ok(DatumKind::UInt8),
            v if DatumKind::Int64.into_u8() == v => Ok(DatumKind::Int64),
            v if DatumKind::Int32.into_u8() == v => Ok(DatumKind::Int32),
            v if DatumKind::Int16.into_u8() == v => Ok(DatumKind::Int16),
            v if DatumKind::Int8.into_u8() == v => Ok(DatumKind::Int8),
            v if DatumKind::Boolean.into_u8() == v => Ok(DatumKind::Boolean),
            _ => InvalidDatumByte { value: v }.fail(),
        }
    }
}

impl From<DatumKind> for DataTypePb {
    fn from(kind: DatumKind) -> Self {
        match kind {
            DatumKind::Null => Self::Null,
            DatumKind::Timestamp => Self::Timestamp,
            DatumKind::Double => Self::Double,
            DatumKind::Float => Self::Float,
            DatumKind::Varbinary => Self::Varbinary,
            DatumKind::String => Self::String,
            DatumKind::UInt64 => Self::Uint64,
            DatumKind::UInt32 => Self::Uint32,
            DatumKind::UInt16 => Self::Uint16,
            DatumKind::UInt8 => Self::Uint8,
            DatumKind::Int64 => Self::Int64,
            DatumKind::Int32 => Self::Int32,
            DatumKind::Int16 => Self::Int16,
            DatumKind::Int8 => Self::Int8,
            DatumKind::Boolean => Self::Bool,
        }
    }
}

impl From<DataTypePb> for DatumKind {
    fn from(data_type: DataTypePb) -> Self {
        match data_type {
            DataTypePb::Null => DatumKind::Null,
            DataTypePb::Timestamp => DatumKind::Timestamp,
            DataTypePb::Double => DatumKind::Double,
            DataTypePb::Float => DatumKind::Float,
            DataTypePb::Varbinary => DatumKind::Varbinary,
            DataTypePb::String => DatumKind::String,
            DataTypePb::Uint64 => DatumKind::UInt64,
            DataTypePb::Uint32 => DatumKind::UInt32,
            DataTypePb::Uint16 => DatumKind::UInt16,
            DataTypePb::Uint8 => DatumKind::UInt8,
            DataTypePb::Int64 => DatumKind::Int64,
            DataTypePb::Int32 => DatumKind::Int32,
            DataTypePb::Int16 => DatumKind::Int16,
            DataTypePb::Int8 => DatumKind::Int8,
            DataTypePb::Bool => DatumKind::Boolean,
        }
    }
}

impl fmt::Display for DatumKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// FIXME(yingwen): Validate the length of string and varbinary.
/// A data box holds different kind of data
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Datum {
    Null,
    /// Millisecond precision
    ///
    /// Map to arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond,
    /// None)
    Timestamp(Timestamp),
    /// Map to arrow::datatypes::DataType::Float64
    Double(f64),
    /// Map to arrow::datatypes::DataType::Float32
    Float(f32),
    /// Map to arrow::datatypes::DateType::Binary
    ///
    /// No more than 2G (size of i32)
    Varbinary(Bytes),
    /// Map to arrow::datatypes::DataType::String
    ///
    /// No more than 2G (size of i32)
    String(StringBytes),
    /// Map to arrow::datatypes::DataType::UInt64
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    Boolean(bool),
}

impl Datum {
    /// Creates an empty datum by given datum kind
    pub fn empty(kind: &DatumKind) -> Self {
        match kind {
            DatumKind::Null => Self::Null,
            DatumKind::Timestamp => Self::Timestamp(Timestamp::new(0)),
            DatumKind::Double => Self::Double(0.0),
            DatumKind::Float => Self::Float(0.0),
            DatumKind::Varbinary => Self::Varbinary(Bytes::new()),
            DatumKind::String => Self::String(StringBytes::new()),
            DatumKind::UInt64 => Self::UInt64(0),
            DatumKind::UInt32 => Self::UInt32(0),
            DatumKind::UInt16 => Self::UInt16(0),
            DatumKind::UInt8 => Self::UInt8(0),
            DatumKind::Int64 => Self::Int64(0),
            DatumKind::Int32 => Self::Int32(0),
            DatumKind::Int16 => Self::Int16(0),
            DatumKind::Int8 => Self::Int8(0),
            DatumKind::Boolean => Self::Boolean(false),
        }
    }

    /// Return the kind of datum
    pub fn kind(&self) -> DatumKind {
        match self {
            Datum::Null => DatumKind::Null,
            Datum::Timestamp(_) => DatumKind::Timestamp,
            Datum::Double(_) => DatumKind::Double,
            Datum::Float(_) => DatumKind::Float,
            Datum::Varbinary(_) => DatumKind::Varbinary,
            Datum::String(_) => DatumKind::String,
            Datum::UInt64(_) => DatumKind::UInt64,
            Datum::UInt32(_) => DatumKind::UInt32,
            Datum::UInt16(_) => DatumKind::UInt16,
            Datum::UInt8(_) => DatumKind::UInt8,
            Datum::Int64(_) => DatumKind::Int64,
            Datum::Int32(_) => DatumKind::Int32,
            Datum::Int16(_) => DatumKind::Int16,
            Datum::Int8(_) => DatumKind::Int8,
            Datum::Boolean(_) => DatumKind::Boolean,
        }
    }

    // TODO: handle error
    pub fn convert_to_uint64(&self) -> u64 {
        match self {
            Datum::Null => 0,
            Datum::Timestamp(v) => v.as_i64() as u64,
            Datum::Double(v) => *v as u64,
            Datum::Float(v) => *v as u64,
            Datum::Varbinary(v) => hash64(v),
            Datum::String(v) => hash64(v.as_bytes()),
            Datum::UInt64(v) => *v,
            Datum::UInt32(v) => *v as u64,
            Datum::UInt16(v) => *v as u64,
            Datum::UInt8(v) => *v as u64,
            Datum::Int64(v) => *v as u64,
            Datum::Int32(v) => *v as u64,
            Datum::Int16(v) => *v as u64,
            Datum::Int8(v) => *v as u64,
            Datum::Boolean(v) => *v as u64,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Cast datum to timestamp.
    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            Datum::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    /// Cast datum to &str.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::String(v) => Some(v),
            _ => None,
        }
    }

    /// Cast datum to uint64.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Datum::UInt64(v) => Some(*v),
            Datum::UInt32(v) => Some(*v as u64),
            Datum::UInt16(v) => Some(*v as u64),
            Datum::UInt8(v) => Some(*v as u64),
            Datum::Int64(v) => Some(*v as u64),
            Datum::Int32(v) => Some(*v as u64),
            Datum::Int16(v) => Some(*v as u64),
            Datum::Int8(v) => Some(*v as u64),
            Datum::Boolean(v) => Some(*v as u64),
            _ => None,
        }
    }

    /// Cast datum to Bytes.
    pub fn as_varbinary(&self) -> Option<&Bytes> {
        match self {
            Datum::Varbinary(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Datum::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Double(v) => Some(*v),
            Datum::Float(v) => Some(*v as f64),
            Datum::UInt64(v) => Some(*v as f64),
            Datum::UInt32(v) => Some(*v as f64),
            Datum::UInt16(v) => Some(*v as f64),
            Datum::UInt8(v) => Some(*v as f64),
            Datum::Int64(v) => Some(*v as f64),
            Datum::Int32(v) => Some(*v as f64),
            Datum::Int16(v) => Some(*v as f64),
            Datum::Int8(v) => Some(*v as f64),
            Datum::Boolean(_)
            | Datum::Null
            | Datum::Timestamp(_)
            | Datum::Varbinary(_)
            | Datum::String(_) => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Datum::Double(v) => v.to_le_bytes().to_vec(),
            Datum::Float(v) => v.to_le_bytes().to_vec(),
            Datum::UInt64(v) => v.to_le_bytes().to_vec(),
            Datum::UInt32(v) => v.to_le_bytes().to_vec(),
            Datum::UInt16(v) => v.to_le_bytes().to_vec(),
            Datum::UInt8(v) => v.to_le_bytes().to_vec(),
            Datum::Int64(v) => v.to_le_bytes().to_vec(),
            Datum::Int32(v) => v.to_le_bytes().to_vec(),
            Datum::Int16(v) => v.to_le_bytes().to_vec(),
            Datum::Int8(v) => v.to_le_bytes().to_vec(),
            Datum::Boolean(v) => {
                if *v {
                    vec![1]
                } else {
                    vec![0]
                }
            }
            Datum::Null => vec![0],
            Datum::Timestamp(ts) => ts.as_i64().to_le_bytes().to_vec(),
            Datum::Varbinary(b) => b.to_vec(),
            Datum::String(string) => string.as_bytes().to_vec(),
        }
    }

    /// Generate a negative datum if possible.
    ///
    /// It will return `None` if:
    /// - The data type has no negative value.
    /// - The negative value overflows.
    pub fn to_negative(self) -> Option<Self> {
        match self {
            Datum::Null => None,
            Datum::Timestamp(_) => None,
            Datum::Double(v) => Some(Datum::Double(-v)),
            Datum::Float(v) => Some(Datum::Float(-v)),
            Datum::Varbinary(_) => None,
            Datum::String(_) => None,
            Datum::UInt64(_) => None,
            Datum::UInt32(_) => None,
            Datum::UInt16(_) => None,
            Datum::UInt8(_) => None,
            Datum::Int64(v) => 0i64.checked_sub(v).map(Datum::Int64),
            Datum::Int32(v) => 0i32.checked_sub(v).map(Datum::Int32),
            Datum::Int16(v) => 0i16.checked_sub(v).map(Datum::Int16),
            Datum::Int8(v) => 0i8.checked_sub(v).map(Datum::Int8),
            Datum::Boolean(v) => Some(Datum::Boolean(!v)),
        }
    }

    pub fn display_string(&self) -> String {
        match self {
            Datum::Null => "null".to_string(),
            Datum::Timestamp(v) => Local.timestamp_millis_opt(v.as_i64()).unwrap().to_rfc3339(),
            Datum::Double(v) => v.to_string(),
            Datum::Float(v) => v.to_string(),
            Datum::Varbinary(v) => format!("{:?}", v),
            Datum::String(v) => v.to_string(),
            Datum::UInt64(v) => v.to_string(),
            Datum::UInt32(v) => v.to_string(),
            Datum::UInt16(v) => v.to_string(),
            Datum::UInt8(v) => v.to_string(),
            Datum::Int64(v) => v.to_string(),
            Datum::Int32(v) => v.to_string(),
            Datum::Int16(v) => v.to_string(),
            Datum::Int8(v) => v.to_string(),
            Datum::Boolean(v) => v.to_string(),
        }
    }

    pub fn try_from_sql_value(kind: &DatumKind, value: Value) -> Result<Datum> {
        match (kind, value) {
            (DatumKind::Null, Value::Null) => Ok(Datum::Null),
            (DatumKind::Timestamp, Value::Number(n, _long)) => {
                let n = n.parse::<i64>().context(InvalidTimestamp)?;
                Ok(Datum::Timestamp(Timestamp::new(n)))
            }
            (DatumKind::Double, Value::Number(n, _long)) => {
                let n = n.parse::<f64>().context(InvalidDouble)?;
                Ok(Datum::Double(n))
            }
            (DatumKind::Float, Value::Number(n, _long)) => {
                let n = n.parse::<f32>().context(InvalidDouble)?;
                Ok(Datum::Float(n))
            }
            // TODO(yingwen): Support hex string.
            (DatumKind::Varbinary, Value::SingleQuotedString(s)) => {
                Ok(Datum::Varbinary(Bytes::from(s)))
            }
            (DatumKind::String, Value::SingleQuotedString(s)) => {
                Ok(Datum::String(StringBytes::from(s)))
            }
            (DatumKind::Varbinary, Value::DoubleQuotedString(s)) => {
                Ok(Datum::Varbinary(Bytes::from(s)))
            }
            (DatumKind::String, Value::DoubleQuotedString(s)) => {
                Ok(Datum::String(StringBytes::from(s)))
            }
            (DatumKind::UInt64, Value::Number(n, _long)) => {
                let n = n.parse::<u64>().context(InvalidInt)?;
                Ok(Datum::UInt64(n))
            }
            (DatumKind::UInt32, Value::Number(n, _long)) => {
                let n = n.parse::<u32>().context(InvalidInt)?;
                Ok(Datum::UInt32(n))
            }
            (DatumKind::UInt16, Value::Number(n, _long)) => {
                let n = n.parse::<u16>().context(InvalidInt)?;
                Ok(Datum::UInt16(n))
            }
            (DatumKind::UInt8, Value::Number(n, _long)) => {
                let n = n.parse::<u8>().context(InvalidInt)?;
                Ok(Datum::UInt8(n))
            }
            (DatumKind::Int64, Value::Number(n, _long)) => {
                let n = n.parse::<i64>().context(InvalidInt)?;
                Ok(Datum::Int64(n))
            }
            (DatumKind::Int32, Value::Number(n, _long)) => {
                let n = n.parse::<i32>().context(InvalidInt)?;
                Ok(Datum::Int32(n))
            }
            (DatumKind::Int16, Value::Number(n, _long)) => {
                let n = n.parse::<i16>().context(InvalidInt)?;
                Ok(Datum::Int16(n))
            }
            (DatumKind::Int8, Value::Number(n, _long)) => {
                let n = n.parse::<i8>().context(InvalidInt)?;
                Ok(Datum::Int8(n))
            }
            (DatumKind::Boolean, Value::Boolean(b)) => Ok(Datum::Boolean(b)),
            (_, value) => InvalidValueType { kind: *kind, value }.fail(),
        }
    }

    #[cfg(test)]
    pub fn as_view(&self) -> DatumView {
        match self {
            Datum::Null => DatumView::Null,
            Datum::Timestamp(v) => DatumView::Timestamp(*v),
            Datum::Double(v) => DatumView::Double(*v),
            Datum::Float(v) => DatumView::Float(*v),
            Datum::Varbinary(v) => DatumView::Varbinary(v),
            Datum::String(v) => DatumView::String(v),
            Datum::UInt64(v) => DatumView::UInt64(*v),
            Datum::UInt32(v) => DatumView::UInt32(*v),
            Datum::UInt16(v) => DatumView::UInt16(*v),
            Datum::UInt8(v) => DatumView::UInt8(*v),
            Datum::Int64(v) => DatumView::Int64(*v),
            Datum::Int32(v) => DatumView::Int32(*v),
            Datum::Int16(v) => DatumView::Int16(*v),
            Datum::Int8(v) => DatumView::Int8(*v),
            Datum::Boolean(v) => DatumView::Boolean(*v),
        }
    }
}

macro_rules! impl_from {
    ($Kind: ident, $FromType: ident) => {
        impl From<$FromType> for Datum {
            fn from(value: $FromType) -> Self {
                Self::$Kind(value)
            }
        }

        impl From<Option<$FromType>> for Datum {
            fn from(value_opt: Option<$FromType>) -> Self {
                match value_opt {
                    Some(value) => Self::$Kind(value),
                    None => Self::Null,
                }
            }
        }
    };
}

impl_from!(Timestamp, Timestamp);
impl_from!(Double, f64);
impl_from!(Float, f32);
impl_from!(Varbinary, Bytes);
impl_from!(String, StringBytes);
impl_from!(UInt64, u64);
impl_from!(UInt32, u32);
impl_from!(UInt16, u16);
impl_from!(UInt8, u8);
impl_from!(Int64, i64);
impl_from!(Int32, i32);
impl_from!(Int16, i16);
impl_from!(Int8, i8);
impl_from!(Boolean, bool);

impl From<&str> for Datum {
    fn from(value: &str) -> Datum {
        Datum::String(StringBytes::copy_from_str(value))
    }
}

impl From<Option<&str>> for Datum {
    fn from(value_opt: Option<&str>) -> Datum {
        match value_opt {
            Some(value) => Datum::String(StringBytes::copy_from_str(value)),
            None => Datum::Null,
        }
    }
}

impl From<&[u8]> for Datum {
    fn from(value: &[u8]) -> Datum {
        Datum::Varbinary(Bytes::copy_from_slice(value))
    }
}

impl From<Option<&[u8]>> for Datum {
    fn from(value_opt: Option<&[u8]>) -> Datum {
        match value_opt {
            Some(value) => Datum::Varbinary(Bytes::copy_from_slice(value)),
            None => Datum::Null,
        }
    }
}

/// impl serde serialize for Datum
impl Serialize for Datum {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Datum::Null => serializer.serialize_none(),
            Datum::Timestamp(v) => serializer.serialize_i64(v.as_i64()),
            Datum::Double(v) => serializer.serialize_f64(*v),
            Datum::Float(v) => serializer.serialize_f32(*v),
            Datum::Varbinary(v) => serializer.serialize_bytes(v),
            Datum::String(v) => serializer.serialize_str(v),
            Datum::UInt64(v) => serializer.serialize_u64(*v),
            Datum::UInt32(v) => serializer.serialize_u32(*v),
            Datum::UInt16(v) => serializer.serialize_u16(*v),
            Datum::UInt8(v) => serializer.serialize_u8(*v),
            Datum::Int64(v) => serializer.serialize_i64(*v),
            Datum::Int32(v) => serializer.serialize_i32(*v),
            Datum::Int16(v) => serializer.serialize_i16(*v),
            Datum::Int8(v) => serializer.serialize_i8(*v),
            Datum::Boolean(v) => serializer.serialize_bool(*v),
        }
    }
}

/// A view to a datum.
///
/// Holds copy of integer like datum and reference of string like datum.
#[derive(Debug, PartialEq, PartialOrd)]
pub enum DatumView<'a> {
    Null,
    Timestamp(Timestamp),
    Double(f64),
    Float(f32),
    Varbinary(&'a [u8]),
    String(&'a str),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    Boolean(bool),
}

impl<'a> DatumView<'a> {
    /// Return the kind of datum
    pub fn kind(&self) -> DatumKind {
        match self {
            DatumView::Null => DatumKind::Null,
            DatumView::Timestamp(_) => DatumKind::Timestamp,
            DatumView::Double(_) => DatumKind::Double,
            DatumView::Float(_) => DatumKind::Float,
            DatumView::Varbinary(_) => DatumKind::Varbinary,
            DatumView::String(_) => DatumKind::String,
            DatumView::UInt64(_) => DatumKind::UInt64,
            DatumView::UInt32(_) => DatumKind::UInt32,
            DatumView::UInt16(_) => DatumKind::UInt16,
            DatumView::UInt8(_) => DatumKind::UInt8,
            DatumView::Int64(_) => DatumKind::Int64,
            DatumView::Int32(_) => DatumKind::Int32,
            DatumView::Int16(_) => DatumKind::Int16,
            DatumView::Int8(_) => DatumKind::Int8,
            DatumView::Boolean(_) => DatumKind::Boolean,
        }
    }
}

#[cfg(feature = "arrow")]
pub mod arrow_convert {
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;

    use super::*;

    impl DatumKind {
        /// Create DatumKind from [arrow::datatypes::DataType], if
        /// the type is not supported, returns None
        pub fn from_data_type(data_type: &DataType) -> Option<Self> {
            match data_type {
                DataType::Null => Some(Self::Null),
                DataType::Timestamp(TimeUnit::Millisecond, None) => Some(Self::Timestamp),
                DataType::Float64 => Some(Self::Double),
                DataType::Float32 => Some(Self::Float),
                DataType::Binary => Some(Self::Varbinary),
                DataType::Utf8 => Some(Self::String),
                DataType::UInt64 => Some(Self::UInt64),
                DataType::UInt32 => Some(Self::UInt32),
                DataType::UInt16 => Some(Self::UInt16),
                DataType::UInt8 => Some(Self::UInt8),
                DataType::Int64 => Some(Self::Int64),
                DataType::Int32 => Some(Self::Int32),
                DataType::Int16 => Some(Self::Int16),
                DataType::Int8 => Some(Self::Int8),
                DataType::Boolean => Some(Self::Boolean),
                DataType::Float16
                | DataType::LargeUtf8
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Struct(_)
                | DataType::Union(_, _, _)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Interval(_)
                | DataType::Duration(_)
                | DataType::Dictionary(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Map(_, _) => None,
            }
        }

        pub fn to_arrow_data_type(&self) -> DataType {
            match self {
                DatumKind::Null => DataType::Null,
                DatumKind::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
                DatumKind::Double => DataType::Float64,
                DatumKind::Float => DataType::Float32,
                DatumKind::Varbinary => DataType::Binary,
                DatumKind::String => DataType::Utf8,
                DatumKind::UInt64 => DataType::UInt64,
                DatumKind::UInt32 => DataType::UInt32,
                DatumKind::UInt16 => DataType::UInt16,
                DatumKind::UInt8 => DataType::UInt8,
                DatumKind::Int64 => DataType::Int64,
                DatumKind::Int32 => DataType::Int32,
                DatumKind::Int16 => DataType::Int16,
                DatumKind::Int8 => DataType::Int8,
                DatumKind::Boolean => DataType::Boolean,
            }
        }
    }

    impl Datum {
        pub fn as_scalar_value(&self) -> Option<ScalarValue> {
            match self {
                Datum::Null => None,
                Datum::Timestamp(v) => {
                    Some(ScalarValue::TimestampMillisecond(Some((*v).as_i64()), None))
                }
                Datum::Double(v) => Some(ScalarValue::Float64(Some(*v))),
                Datum::Float(v) => Some(ScalarValue::Float32(Some(*v))),
                Datum::Varbinary(v) => Some(ScalarValue::Binary(Some(v.to_vec()))),
                Datum::String(v) => Some(ScalarValue::Utf8(Some(v.to_string()))),
                Datum::UInt64(v) => Some(ScalarValue::UInt64(Some(*v))),
                Datum::UInt32(v) => Some(ScalarValue::UInt32(Some(*v))),
                Datum::UInt16(v) => Some(ScalarValue::UInt16(Some(*v))),
                Datum::UInt8(v) => Some(ScalarValue::UInt8(Some(*v))),
                Datum::Int64(v) => Some(ScalarValue::Int64(Some(*v))),
                Datum::Int32(v) => Some(ScalarValue::Int32(Some(*v))),
                Datum::Int16(v) => Some(ScalarValue::Int16(Some(*v))),
                Datum::Int8(v) => Some(ScalarValue::Int8(Some(*v))),
                Datum::Boolean(v) => Some(ScalarValue::Boolean(Some(*v))),
            }
        }

        pub fn from_scalar_value(val: &ScalarValue) -> Option<Self> {
            match val {
                ScalarValue::Boolean(v) => v.map(Datum::Boolean),
                ScalarValue::Float32(v) => v.map(Datum::Float),
                ScalarValue::Float64(v) => v.map(Datum::Double),
                ScalarValue::Int8(v) => v.map(Datum::Int8),
                ScalarValue::Int16(v) => v.map(Datum::Int16),
                ScalarValue::Int32(v) => v.map(Datum::Int32),
                ScalarValue::Int64(v) => v.map(Datum::Int64),
                ScalarValue::UInt8(v) => v.map(Datum::UInt8),
                ScalarValue::UInt16(v) => v.map(Datum::UInt16),
                ScalarValue::UInt32(v) => v.map(Datum::UInt32),
                ScalarValue::UInt64(v) => v.map(Datum::UInt64),
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v
                    .as_ref()
                    .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str()))),
                ScalarValue::Binary(v)
                | ScalarValue::FixedSizeBinary(_, v)
                | ScalarValue::LargeBinary(v) => v
                    .as_ref()
                    .map(|v| Datum::Varbinary(Bytes::copy_from_slice(v.as_slice()))),
                ScalarValue::TimestampMillisecond(v, _) => {
                    v.map(|v| Datum::Timestamp(Timestamp::new(v)))
                }
                ScalarValue::List(_, _)
                | ScalarValue::Date32(_)
                | ScalarValue::Date64(_)
                | ScalarValue::Time32Second(_)
                | ScalarValue::Time32Millisecond(_)
                | ScalarValue::Time64Microsecond(_)
                | ScalarValue::Time64Nanosecond(_)
                | ScalarValue::TimestampSecond(_, _)
                | ScalarValue::TimestampMicrosecond(_, _)
                | ScalarValue::TimestampNanosecond(_, _)
                | ScalarValue::IntervalYearMonth(_)
                | ScalarValue::IntervalDayTime(_)
                | ScalarValue::Struct(_, _)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Null
                | ScalarValue::IntervalMonthDayNano(_)
                | ScalarValue::Dictionary(_, _) => None,
            }
        }
    }

    impl<'a> DatumView<'a> {
        pub fn from_scalar_value(val: &'a ScalarValue) -> Option<Self> {
            match val {
                ScalarValue::Boolean(v) => v.map(DatumView::Boolean),
                ScalarValue::Float32(v) => v.map(DatumView::Float),
                ScalarValue::Float64(v) => v.map(DatumView::Double),
                ScalarValue::Int8(v) => v.map(DatumView::Int8),
                ScalarValue::Int16(v) => v.map(DatumView::Int16),
                ScalarValue::Int32(v) => v.map(DatumView::Int32),
                ScalarValue::Int64(v) => v.map(DatumView::Int64),
                ScalarValue::UInt8(v) => v.map(DatumView::UInt8),
                ScalarValue::UInt16(v) => v.map(DatumView::UInt16),
                ScalarValue::UInt32(v) => v.map(DatumView::UInt32),
                ScalarValue::UInt64(v) => v.map(DatumView::UInt64),
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                    v.as_ref().map(|v| DatumView::String(v.as_str()))
                }
                ScalarValue::Binary(v)
                | ScalarValue::FixedSizeBinary(_, v)
                | ScalarValue::LargeBinary(v) => {
                    v.as_ref().map(|v| DatumView::Varbinary(v.as_slice()))
                }
                ScalarValue::TimestampMillisecond(v, _) => {
                    v.map(|v| DatumView::Timestamp(Timestamp::new(v)))
                }
                ScalarValue::List(_, _)
                | ScalarValue::Date32(_)
                | ScalarValue::Date64(_)
                | ScalarValue::Time32Second(_)
                | ScalarValue::Time32Millisecond(_)
                | ScalarValue::Time64Microsecond(_)
                | ScalarValue::Time64Nanosecond(_)
                | ScalarValue::TimestampSecond(_, _)
                | ScalarValue::TimestampMicrosecond(_, _)
                | ScalarValue::TimestampNanosecond(_, _)
                | ScalarValue::IntervalYearMonth(_)
                | ScalarValue::IntervalDayTime(_)
                | ScalarValue::Struct(_, _)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Null
                | ScalarValue::IntervalMonthDayNano(_)
                | ScalarValue::Dictionary(_, _) => None,
            }
        }
    }

    impl From<DatumKind> for DataType {
        fn from(kind: DatumKind) -> Self {
            match kind {
                DatumKind::Null => DataType::Null,
                DatumKind::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
                DatumKind::Double => DataType::Float64,
                DatumKind::Float => DataType::Float32,
                DatumKind::Varbinary => DataType::Binary,
                DatumKind::String => DataType::Utf8,
                DatumKind::UInt64 => DataType::UInt64,
                DatumKind::UInt32 => DataType::UInt32,
                DatumKind::UInt16 => DataType::UInt16,
                DatumKind::UInt8 => DataType::UInt8,
                DatumKind::Int64 => DataType::Int64,
                DatumKind::Int32 => DataType::Int32,
                DatumKind::Int16 => DataType::Int16,
                DatumKind::Int8 => DataType::Int8,
                DatumKind::Boolean => DataType::Boolean,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_key_kind() {
        assert!(!DatumKind::Null.is_key_kind());
        assert!(DatumKind::Timestamp.is_key_kind());
        assert!(!DatumKind::Double.is_key_kind());
        assert!(!DatumKind::Float.is_key_kind());
        assert!(DatumKind::Varbinary.is_key_kind());
        assert!(DatumKind::String.is_key_kind());
        assert!(DatumKind::UInt64.is_key_kind());
        assert!(DatumKind::UInt32.is_key_kind());
        assert!(DatumKind::UInt16.is_key_kind());
        assert!(DatumKind::UInt8.is_key_kind());
        assert!(DatumKind::Int64.is_key_kind());
        assert!(DatumKind::Int32.is_key_kind());
        assert!(DatumKind::Int16.is_key_kind());
        assert!(DatumKind::Int8.is_key_kind());
        assert!(DatumKind::Boolean.is_key_kind());
    }

    #[test]
    fn test_unsign_kind() {
        assert_eq!(DatumKind::UInt64.unsign_kind(), Some(DatumKind::UInt64));
        assert_eq!(DatumKind::Int64.unsign_kind(), Some(DatumKind::UInt64));
        assert_eq!(DatumKind::UInt32.unsign_kind(), Some(DatumKind::UInt32));
        assert_eq!(DatumKind::Int32.unsign_kind(), Some(DatumKind::UInt32));
        assert_eq!(DatumKind::UInt16.unsign_kind(), Some(DatumKind::UInt16));
        assert_eq!(DatumKind::Int16.unsign_kind(), Some(DatumKind::UInt16));
        assert_eq!(DatumKind::UInt8.unsign_kind(), Some(DatumKind::UInt8));
        assert_eq!(DatumKind::Int8.unsign_kind(), Some(DatumKind::UInt8));

        assert!(DatumKind::Null.unsign_kind().is_none());
        assert!(DatumKind::Timestamp.unsign_kind().is_none());
        assert!(DatumKind::String.unsign_kind().is_none());
        assert!(DatumKind::Boolean.unsign_kind().is_none());
        assert!(DatumKind::Varbinary.unsign_kind().is_none());
        assert!(DatumKind::Double.unsign_kind().is_none());
        assert!(DatumKind::Float.unsign_kind().is_none());
    }

    #[test]
    fn test_into_u8() {
        assert_eq!(0, DatumKind::Null.into_u8());
        assert_eq!(1, DatumKind::Timestamp.into_u8());
        assert_eq!(2, DatumKind::Double.into_u8());
        assert_eq!(3, DatumKind::Float.into_u8());
        assert_eq!(4, DatumKind::Varbinary.into_u8());
        assert_eq!(5, DatumKind::String.into_u8());
        assert_eq!(6, DatumKind::UInt64.into_u8());
        assert_eq!(7, DatumKind::UInt32.into_u8());
        assert_eq!(8, DatumKind::UInt16.into_u8());
        assert_eq!(9, DatumKind::UInt8.into_u8());
        assert_eq!(10, DatumKind::Int64.into_u8());
        assert_eq!(11, DatumKind::Int32.into_u8());
        assert_eq!(12, DatumKind::Int16.into_u8());
        assert_eq!(13, DatumKind::Int8.into_u8());
        assert_eq!(14, DatumKind::Boolean.into_u8());
    }

    #[test]
    fn test_to_negative_value() {
        let cases = [
            (Datum::Null, None),
            (Datum::Timestamp(Timestamp::ZERO), None),
            (Datum::Double(1.0), Some(Datum::Double(-1.0))),
            (Datum::Float(1.0), Some(Datum::Float(-1.0))),
            (Datum::Varbinary(Bytes::new()), None),
            (Datum::String(StringBytes::new()), None),
            (Datum::UInt64(10), None),
            (Datum::UInt32(10), None),
            (Datum::UInt16(10), None),
            (Datum::UInt8(10), None),
            (Datum::Int64(10), Some(Datum::Int64(-10))),
            (Datum::Int32(10), Some(Datum::Int32(-10))),
            (Datum::Int16(10), Some(Datum::Int16(-10))),
            (Datum::Int8(10), Some(Datum::Int8(-10))),
        ];

        for (source, negative) in cases {
            assert_eq!(negative, source.to_negative());
        }
    }

    #[test]
    fn test_to_overflow_negative_value() {
        let cases = [
            Datum::Int64(i64::MIN),
            Datum::Int32(i32::MIN),
            Datum::Int16(i16::MIN),
            Datum::Int8(i8::MIN),
        ];

        for source in cases {
            assert!(source.to_negative().is_none());
        }
    }
}
