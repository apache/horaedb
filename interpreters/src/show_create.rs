// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, convert::TryInto, sync::Arc};

use arrow_deps::arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use query_engine::executor::RecordBatchVec;
use snafu::ensure;
use sql::{ast::ShowCreateObject, plan::ShowCreatePlan};
use table_engine::table::TableRef;

use crate::{
    interpreter::Output,
    show::{Result, UnsupportedType},
};

pub struct ShowCreateInterpreter {
    plan: ShowCreatePlan,
}

impl ShowCreateInterpreter {
    pub fn create(plan: ShowCreatePlan) -> ShowCreateInterpreter {
        Self { plan }
    }

    pub fn execute_show_create(self) -> Result<Output> {
        let ShowCreatePlan { table, obj_type } = self.plan;

        ensure!(
            obj_type == ShowCreateObject::Table,
            UnsupportedType { obj_type }
        );

        Self::table_ref_to_record_batch(table).map(Output::Records)
    }

    fn table_ref_to_record_batch(table_ref: TableRef) -> Result<RecordBatchVec> {
        let tables = vec![table_ref.name().to_string()];
        let sqls = vec![Self::render_table_sql(table_ref)];

        let schema = Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ]);

        let arrow_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(tables)),
                Arc::new(StringArray::from(sqls)),
            ],
        )
        .unwrap();

        let record_batch = arrow_record_batch.try_into().unwrap();

        Ok(vec![record_batch])
    }

    fn render_table_sql(table_ref: TableRef) -> String {
        //TODO(boyan) pretty output
        format!(
            "CREATE TABLE `{}` ({}) ENGINE={}{}",
            table_ref.name(),
            Self::render_columns_and_constrains(&table_ref),
            table_ref.engine_type(),
            Self::render_options(table_ref.options())
        )
    }

    fn render_columns_and_constrains(table_ref: &TableRef) -> String {
        let table_schema = table_ref.schema();
        let key_columns = table_schema.key_columns();
        let timestamp_key = table_schema.timestamp_name();

        let mut res = String::new();
        for col in table_schema.columns() {
            println!("{:?}", col);
            res += format!("`{}` {}", col.name, col.data_type).as_str();
            if col.is_tag {
                res += " TAG";
            }
            if !col.is_nullable {
                res += " NOT NULL";
            }

            if let Some(expr) = &col.default_value {
                res += format!(" DEFAULT {}", expr).as_str();
            }

            if !col.comment.is_empty() {
                res += format!(" COMMENT '{}'", col.comment).as_str();
            }
            res += ", ";
        }
        let keys: Vec<String> = key_columns.iter().map(|col| col.name.to_string()).collect();
        res += format!("PRIMARY KEY({}), ", keys.join(",")).as_str();
        res += format!("TIMESTAMP KEY({})", timestamp_key).as_str();

        res
    }

    fn render_options(opts: HashMap<String, String>) -> String {
        if !opts.is_empty() {
            let mut v: Vec<String> = opts
                .into_iter()
                .map(|(k, v)| format!("{}='{}'", k, v))
                .collect();
            // sorted by option name
            v.sort();
            format!(" WITH({})", v.join(", "))
        } else {
            "".to_string()
        }
    }
}
