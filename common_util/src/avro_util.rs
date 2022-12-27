// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Avro utility

use std::collections::HashMap;

use avro_rs::{
    schema::{Name, RecordField, RecordFieldOrder},
    types::{Record, Value},
};
use common_types::{
    bytes::{ByteVec, Bytes},
    column::ColumnBlock,
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
    record_batch::RecordBatch,
    schema::RecordSchema,
    string::StringBytes,
    time::Timestamp,
};
use snafu::{Backtrace, ResultExt, Snafu};

use crate::define_result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to write avro record, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    WriteAvroRecord {
        source: avro_rs::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert avro raw to row, err:{}.", source,))]
    RawToRowWithCause { source: avro_rs::Error },

    #[snafu(display(
        "Failed to convert avro raw to row, msg:{}.\nBacktrace:\n{}",
        msg,
        backtrace
    ))]
    RawToRowNoCause { msg: String, backtrace: Backtrace },
}

define_result!(Error);

/// Create [avro_rs::Schema] with given `name` from [RecordSchema]
pub fn to_avro_schema(name: &str, schema: &RecordSchema) -> avro_rs::Schema {
    let columns = schema.columns();
    columns_to_avro_schema(name, columns)
}

pub fn columns_to_avro_schema(name: &str, columns: &[ColumnSchema]) -> avro_rs::Schema {
    let mut lookup = HashMap::with_capacity(columns.len());
    let mut avro_fields = Vec::with_capacity(columns.len());

    for (pos, column) in columns.iter().enumerate() {
        // Create avro record field
        let default = if column.is_nullable {
            Some(serde_json::value::Value::Null)
        } else {
            None
        };

        let field_schema = if column.is_nullable {
            // We want to declare a schema which may be either a null or non-null value,
            // for example: ["null", "string"].
            //
            // However, `avro_rs` does not provide an accessible API to build a `Union`.
            // We can't find a better way to do this than using JSON.
            let field_schema_str = format!(
                r#"["null", {}]"#,
                data_type_to_schema(&column.data_type).canonical_form()
            );
            avro_rs::Schema::parse_str(&field_schema_str).unwrap()
        } else {
            data_type_to_schema(&column.data_type)
        };

        // In dummy select like select "xxx", column name will be "Utf8("xxx")", which
        // is not a valid json string. So, escaped name is used here.
        let record_field = RecordField {
            name: column.escaped_name.clone(),
            doc: None,
            default,
            schema: field_schema,
            order: RecordFieldOrder::Ignore,
            position: pos,
        };

        avro_fields.push(record_field);
        lookup.insert(column.escaped_name.clone(), pos);
    }

    avro_rs::Schema::Record {
        name: Name::new(name),
        doc: None,
        fields: avro_fields,
        lookup,
    }
}

fn data_type_to_schema(data_type: &DatumKind) -> avro_rs::Schema {
    match data_type {
        DatumKind::Null => avro_rs::Schema::Null,
        DatumKind::Timestamp => avro_rs::Schema::TimestampMillis,
        DatumKind::Double => avro_rs::Schema::Double,
        DatumKind::Float => avro_rs::Schema::Float,
        DatumKind::Varbinary => avro_rs::Schema::Bytes,
        DatumKind::String => avro_rs::Schema::String,
        DatumKind::UInt32 | DatumKind::Int64 | DatumKind::UInt64 => avro_rs::Schema::Long,
        DatumKind::UInt16
        | DatumKind::UInt8
        | DatumKind::Int32
        | DatumKind::Int16
        | DatumKind::Int8 => avro_rs::Schema::Int,
        DatumKind::Boolean => avro_rs::Schema::Boolean,
    }
}

/// Convert record batch to avro format
pub fn record_batch_to_avro(
    record_batch: &RecordBatch,
    schema: &avro_rs::Schema,
    rows: &mut Vec<ByteVec>,
) -> Result<()> {
    let record_batch_schema = record_batch.schema();
    assert_eq!(
        record_batch_schema.num_columns(),
        record_batch.num_columns()
    );

    rows.reserve(record_batch.num_rows());

    let column_schemas = record_batch_schema.columns();
    for row_idx in 0..record_batch.num_rows() {
        let mut record = Record::new(schema).unwrap();
        for (col_idx, column_schema) in column_schemas.iter().enumerate() {
            let column = record_batch.column(col_idx);
            let value = column_to_value(column, row_idx, column_schema.is_nullable);

            record.put(&column_schema.escaped_name, value);
        }

        let row_bytes = avro_rs::to_avro_datum(schema, record).context(WriteAvroRecord)?;

        rows.push(row_bytes);
    }

    Ok(())
}

/// Panic if row_idx is out of bound.
fn column_to_value(array: &ColumnBlock, row_idx: usize, is_nullable: bool) -> Value {
    let datum = array.datum(row_idx);
    datum_to_value(datum, is_nullable)
}

pub fn datum_to_value(datum: Datum, is_nullable: bool) -> Value {
    match datum {
        Datum::Null => may_union(Value::Null, is_nullable),
        Datum::Timestamp(v) => may_union(Value::TimestampMillis(v.as_i64()), is_nullable),
        Datum::Double(v) => may_union(Value::Double(v), is_nullable),
        Datum::Float(v) => may_union(Value::Float(v), is_nullable),
        Datum::Varbinary(v) => may_union(Value::Bytes(v.to_vec()), is_nullable),
        Datum::String(v) => may_union(Value::String(v.to_string()), is_nullable),
        // TODO(yingwen): Should we return error if overflow? Avro does not support uint64.
        Datum::UInt64(v) => may_union(Value::Long(v as i64), is_nullable),
        Datum::Int64(v) => may_union(Value::Long(v), is_nullable),
        Datum::UInt32(v) => may_union(Value::Long(i64::from(v)), is_nullable),
        Datum::UInt16(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::UInt8(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Int32(v) => may_union(Value::Int(v), is_nullable),
        Datum::Int16(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Int8(v) => may_union(Value::Int(i32::from(v)), is_nullable),
        Datum::Boolean(v) => may_union(Value::Boolean(v), is_nullable),
    }
}

#[inline]
fn may_union(val: Value, is_nullable: bool) -> Value {
    if is_nullable {
        Value::Union(Box::new(val))
    } else {
        val
    }
}

pub fn raw_to_row(schema: &avro_rs::Schema, mut raw: &[u8], row: &mut Vec<Datum>) -> Result<()> {
    let record = avro_rs::from_avro_datum(schema, &mut raw, None).context(RawToRowWithCause)?;

    if let Value::Record(cols) = record {
        for (_, column_value) in cols {
            let datum = value_to_datum(column_value)?;
            row.push(datum);
        }

        Ok(())
    } else {
        RawToRowNoCause {
            msg: "invalid avro record",
        }
        .fail()
    }
}

fn value_to_datum(value: Value) -> Result<Datum> {
    let datum = match value {
        Value::Null => Datum::Null,
        Value::TimestampMillis(v) => Datum::Timestamp(Timestamp::new(v)),
        Value::Double(v) => Datum::Double(v),
        Value::Float(v) => Datum::Float(v),
        Value::Bytes(v) => Datum::Varbinary(Bytes::from(v)),
        Value::String(v) => Datum::String(StringBytes::from(v)),
        // FIXME: Now the server converts both uint64 and int64 into`Value::Long` because uint64 is
        // not supported by avro, that is to say something may go wrong in some corner case.
        Value::Long(v) => Datum::Int64(v),
        Value::Int(v) => Datum::Int32(v),
        Value::Boolean(v) => Datum::Boolean(v),
        Value::Union(inner_val) => value_to_datum(*inner_val)?,
        Value::Fixed(_, _)
        | Value::Enum(_, _)
        | Value::Array(_)
        | Value::Map(_)
        | Value::Record(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::TimeMillis(_)
        | Value::TimeMicros(_)
        | Value::TimestampMicros(_)
        | Value::Duration(_)
        | Value::Uuid(_) => {
            return RawToRowNoCause {
                msg: "invalid avro value type",
            }
            .fail()
        }
    };

    Ok(datum)
}
