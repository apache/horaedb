// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Influxql schema provider

use std::sync::Arc;

use arrow::datatypes::Field as ArrowField;
use common_types::{column_schema::ColumnSchema, datum::DatumKind, schema::Schema};
use common_util::error::BoxError;
use datafusion::sql::planner::ContextProvider;
use influxql_logical_planner::{
    provider::{InfluxColumnType, InfluxFieldType, Schema as InfluxSchema, SchemaProvider},
    DataFusionError, Result as DatafusionResult,
};

use crate::{
    influxql::error::*,
    provider::{ContextProviderAdapter, MetaProvider},
};

/// Influx schema used for build logical plan
pub struct InfluxSchemaImpl {
    columns: Vec<InfluxColumnSchema>,
    time_column_idx: usize,
}

impl InfluxSchemaImpl {
    /// New influx schema by ceresdb inner schema.
    ///
    /// NOTICE: The compatible ceresdb inner schema is required as following:
    ///     + Only one timestamp column named "time"
    ///     + Tag column only can be string type and nullable
    ///     + Field column only can be int64/uint64/float64/string/boolean type
    /// and nullable
    fn new(schema: &Schema) -> Result<Self> {
        let cols = schema.columns();
        let timestamp_key_idx = schema.timestamp_index();
        let tsid_idx_opt = schema.index_of_tsid();
        let arrow_fields = &schema.to_arrow_schema_ref().fields;

        let influx_columns = arrow_fields
            .iter()
            .zip(cols.iter().enumerate())
            .filter_map(|(arrow_col, (col_idx, col))| {
                if matches!(tsid_idx_opt, Some(tsid_idx) if col_idx == tsid_idx) {
                    None
                } else {
                    let influx_type_res =
                        map_column_to_influx_column(col, col_idx == timestamp_key_idx);
                    Some(influx_type_res.map(|influx_type| InfluxColumnSchema {
                        influx_type,
                        arrow_field: arrow_col.clone(),
                    }))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // Schema is ensured to have timestamp key.
        let time_column_idx = influx_columns
            .iter()
            .enumerate()
            .find(|(_, column)| matches!(column.influx_type, InfluxColumnType::Timestamp))
            .map(|(idx, _)| idx)
            .unwrap();

        Ok(Self {
            columns: influx_columns,
            time_column_idx,
        })
    }
}

impl InfluxSchema for InfluxSchemaImpl {
    fn columns(&self) -> Vec<(InfluxColumnType, &ArrowField)> {
        self.columns
            .iter()
            .map(|column| (column.influx_type, &column.arrow_field))
            .collect()
    }

    fn tags(&self) -> Vec<&ArrowField> {
        self.columns
            .iter()
            .filter_map(|column| {
                if matches!(column.influx_type, InfluxColumnType::Tag) {
                    Some(&column.arrow_field)
                } else {
                    None
                }
            })
            .collect()
    }

    fn fields(&self) -> Vec<&ArrowField> {
        self.columns
            .iter()
            .filter_map(|column| {
                if matches!(column.influx_type, InfluxColumnType::Field(..)) {
                    Some(&column.arrow_field)
                } else {
                    None
                }
            })
            .collect()
    }

    fn time(&self) -> &ArrowField {
        // Time column must exist, has checked it when building.
        let time_column = &self.columns[self.time_column_idx];

        &time_column.arrow_field
    }

    fn column(&self, idx: usize) -> (InfluxColumnType, &ArrowField) {
        let column = &self.columns[idx];

        (column.influx_type, &column.arrow_field)
    }

    fn find_index_of(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.arrow_field.name() == name)
            .map(|(index, _)| index)
    }
}

fn map_column_to_influx_column(
    column: &ColumnSchema,
    is_timestamp_key: bool,
) -> Result<InfluxColumnType> {
    if is_timestamp_key {
        return map_column_to_influx_time_column(column);
    }

    if column.is_tag {
        return map_column_to_influx_tag_column(column);
    }

    map_column_to_influx_field_column(column)
}

// TODO: don't restrict the time column name.
fn map_column_to_influx_time_column(column: &ColumnSchema) -> Result<InfluxColumnType> {
    if column.name == "time" && !column.is_nullable {
        Ok(InfluxColumnType::Timestamp)
    } else {
        BuildSchema {
            msg: format!("invalid time column, column:{column:?}"),
        }
        .fail()
    }
}

// TODO: support more tag types.
fn map_column_to_influx_tag_column(column: &ColumnSchema) -> Result<InfluxColumnType> {
    if matches!(column.data_type, DatumKind::String) && column.is_nullable {
        Ok(InfluxColumnType::Tag)
    } else {
        BuildSchema {
            msg: format!("invalid tag column, column:{column:?}"),
        }
        .fail()
    }
}

// TODO: support more field types.
fn map_column_to_influx_field_column(column: &ColumnSchema) -> Result<InfluxColumnType> {
    if column.is_nullable {
        match column.data_type {
            DatumKind::Int64 => Ok(InfluxColumnType::Field(InfluxFieldType::Integer)),
            DatumKind::UInt64 => Ok(InfluxColumnType::Field(InfluxFieldType::UInteger)),
            DatumKind::Double => Ok(InfluxColumnType::Field(InfluxFieldType::Float)),
            DatumKind::String => Ok(InfluxColumnType::Field(InfluxFieldType::String)),
            DatumKind::Boolean => Ok(InfluxColumnType::Field(InfluxFieldType::Boolean)),
            DatumKind::Null
            | DatumKind::Timestamp
            | DatumKind::Float
            | DatumKind::Varbinary
            | DatumKind::UInt32
            | DatumKind::UInt16
            | DatumKind::UInt8
            | DatumKind::Int32
            | DatumKind::Int16
            | DatumKind::Int8
            | DatumKind::Date
            | DatumKind::Time => BuildSchema {
                msg: format!("invalid field column, column:{column:?}"),
            }
            .fail(),
        }
    } else {
        BuildSchema {
            msg: format!("invalid field column, column:{column:?}"),
        }
        .fail()
    }
}

struct InfluxColumnSchema {
    influx_type: InfluxColumnType,
    arrow_field: ArrowField,
}

/// Influx schema provider used for building logical plan
pub(crate) struct InfluxSchemaProviderImpl<'a, P: MetaProvider> {
    pub(crate) context_provider: &'a ContextProviderAdapter<'a, P>,
}

impl<'a, P: MetaProvider> SchemaProvider for InfluxSchemaProviderImpl<'a, P> {
    fn get_table_provider(
        &self,
        name: &str,
    ) -> DatafusionResult<Arc<dyn datafusion_expr::TableSource>> {
        self.context_provider
            .get_table_provider(name.into())
            .box_err()
            .map_err(|e| DataFusionError::External(e))
    }

    fn table_names(&self) -> DatafusionResult<Vec<&'_ str>> {
        Err(DataFusionError::NotImplemented(
            "get all table names".to_string(),
        ))
    }

    fn table_schema(
        &self,
        name: &str,
    ) -> DatafusionResult<Option<std::sync::Arc<dyn InfluxSchema>>> {
        let table_opt = self
            .context_provider
            .table(name.into())
            .box_err()
            .map_err(|e| DataFusionError::External(e))?;

        Ok(match table_opt {
            Some(table) => {
                let influx_schema = InfluxSchemaImpl::new(&table.schema())
                    .box_err()
                    .map_err(|e| DataFusionError::External(e))?;
                Some(Arc::new(influx_schema))
            }
            None => None,
        })
    }

    fn table_exists(&self, name: &str) -> DatafusionResult<bool> {
        Ok(self.table_schema(name)?.is_some())
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, TimeUnit};
    use common_types::{
        column_schema,
        datum::DatumKind,
        schema::{self, Schema, TSID_COLUMN},
    };
    use influxql_logical_planner::provider::{
        InfluxColumnType, InfluxFieldType, Schema as InfluxSchema,
    };

    use super::InfluxSchemaImpl;

    #[test]
    fn test_build_influx_schema() {
        let cases = vec![
            Case::Compatible,
            Case::TimeNameInvalid,
            Case::TagNotNull,
            Case::FieldNotNull,
            Case::TagTypeInvalid,
            Case::FieldTypeInvalid,
        ];

        for case in cases {
            let schema = build_test_schema(case);
            let influx_schema = InfluxSchemaImpl::new(&schema);
            match case {
                Case::Compatible => {
                    let influx_schema = influx_schema.unwrap();
                    let columns = influx_schema.columns();
                    for column in columns {
                        match column {
                            (InfluxColumnType::Timestamp, field) => {
                                assert_eq!(field.name(), "time");
                                assert_eq!(
                                    field.data_type(),
                                    &DataType::Timestamp(TimeUnit::Millisecond, None)
                                );
                                assert!(!field.is_nullable());
                            }
                            (InfluxColumnType::Tag, field) => {
                                assert_eq!(field.name(), "tag");
                                assert_eq!(field.data_type(), &DataType::Utf8);
                                assert!(field.is_nullable());
                            }
                            (InfluxColumnType::Field(InfluxFieldType::Integer), field) => {
                                assert_eq!(field.name(), "int_field");
                                assert_eq!(field.data_type(), &DataType::Int64);
                                assert!(field.is_nullable());
                            }
                            (InfluxColumnType::Field(InfluxFieldType::UInteger), field) => {
                                assert_eq!(field.name(), "uint_field");
                                assert_eq!(field.data_type(), &DataType::UInt64);
                                assert!(field.is_nullable());
                            }
                            (InfluxColumnType::Field(InfluxFieldType::Float), field) => {
                                assert_eq!(field.name(), "float_field");
                                assert_eq!(field.data_type(), &DataType::Float64);
                                assert!(field.is_nullable());
                            }
                            (InfluxColumnType::Field(InfluxFieldType::String), field) => {
                                assert_eq!(field.name(), "str_field");
                                assert_eq!(field.data_type(), &DataType::Utf8);
                                assert!(field.is_nullable());
                            }
                            (InfluxColumnType::Field(InfluxFieldType::Boolean), field) => {
                                assert_eq!(field.name(), "bool_field");
                                assert_eq!(field.data_type(), &DataType::Boolean);
                                assert!(field.is_nullable());
                            }
                        }
                    }
                }
                Case::TimeNameInvalid => {
                    assert!(influx_schema.is_err());
                    assert!(influx_schema
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("invalid time column"));
                }
                Case::TagNotNull => {
                    assert!(influx_schema.is_err());
                    assert!(influx_schema
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("invalid tag column"));
                }
                Case::FieldNotNull => {
                    assert!(influx_schema.is_err());
                    assert!(influx_schema
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("invalid field column"));
                }
                Case::TagTypeInvalid => {
                    assert!(influx_schema.is_err());
                    assert!(influx_schema
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("invalid tag column"));
                }
                Case::FieldTypeInvalid => {
                    assert!(influx_schema.is_err());
                    assert!(influx_schema
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("invalid field column"));
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Case {
        Compatible,
        TimeNameInvalid,
        TagNotNull,
        FieldNotNull,
        TagTypeInvalid,
        FieldTypeInvalid,
    }

    fn build_test_schema(case: Case) -> Schema {
        let time_column_name = if matches!(case, Case::TimeNameInvalid) {
            "not_time"
        } else {
            "time"
        };

        let base_schema_builder = schema::Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new(time_column_name.to_string(), DatumKind::Timestamp)
                    .is_nullable(false)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(TSID_COLUMN.to_owned(), DatumKind::UInt64)
                    .is_nullable(false)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("tag".to_string(), DatumKind::String)
                    .is_nullable(true)
                    .is_tag(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("str_field".to_string(), DatumKind::String)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("int_field".to_string(), DatumKind::Int64)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("uint_field".to_string(), DatumKind::UInt64)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("float_field".to_string(), DatumKind::Double)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new("bool_field".to_string(), DatumKind::Boolean)
                    .is_nullable(true)
                    .build()
                    .expect("should succeed build column schema"),
            )
            .unwrap();

        let schema_builder = match case {
            Case::TagNotNull => base_schema_builder
                .add_normal_column(
                    column_schema::Builder::new("tag_not_null".to_string(), DatumKind::String)
                        .is_nullable(false)
                        .is_tag(true)
                        .build()
                        .expect("should succeed build column schema"),
                )
                .unwrap(),
            Case::FieldNotNull => base_schema_builder
                .add_normal_column(
                    column_schema::Builder::new("field_not_null".to_string(), DatumKind::Int64)
                        .is_nullable(false)
                        .build()
                        .expect("should succeed build column schema"),
                )
                .unwrap(),
            Case::TagTypeInvalid => base_schema_builder
                .add_normal_column(
                    column_schema::Builder::new("tag_invaild".to_string(), DatumKind::Varbinary)
                        .is_nullable(true)
                        .is_tag(true)
                        .build()
                        .expect("should succeed build column schema"),
                )
                .unwrap(),
            Case::FieldTypeInvalid => base_schema_builder
                .add_normal_column(
                    column_schema::Builder::new("field_invalid".to_string(), DatumKind::Varbinary)
                        .is_nullable(true)
                        .build()
                        .expect("should succeed build column schema"),
                )
                .unwrap(),
            _ => base_schema_builder,
        };

        schema_builder.build().expect("should succeed build schema")
    }
}
