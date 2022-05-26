// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

/// implementation of system table: Tables
/// For example `SELECT * FROM system.public.tables`
use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use catalog::{manager::Manager, schema::SchemaRef, CatalogRef};
use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    record_batch::RecordBatchWithKeyBuilder,
    row::Row,
    schema,
    schema::Schema,
};
use snafu::ResultExt;
use table_engine::{
    stream::SendableRecordBatchStream,
    table::{ReadRequest, SchemaId, TableId, TableRef, TableSeq},
};

use crate::{OneRecordBatchStream, SystemTable, ENTRY_TIMESTAMP};

/// Table name of the sys tables
const TABLE_NAME: &str = "tables";
/// Schema id of the sys catalog schema (`system/public`).
pub const SCHEMA_ID: SchemaId = SchemaId::from_u16(1);
/// Table sequence of the sys tables
pub const TABLE_SEQ: TableSeq = TableSeq::from_u32(2);
/// Table id of the `sys_catalog` table.
pub const TABLE_ID: TableId = TableId::new(SCHEMA_ID, TABLE_SEQ);

/// Build a new table schema for tables
fn tables_schema() -> Schema {
    schema::Builder::with_capacity(6)
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("catalog".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("schema".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("table_name".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("table_id".to_string(), DatumKind::UInt64)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("engine".to_string(), DatumKind::String)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
}

pub struct Tables<M> {
    schema: Schema,
    catalog_manager: M,
}

impl<M> Debug for Tables<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SysTables")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<M: Manager> Tables<M> {
    pub fn new(catalog_manager: M) -> Self {
        Self {
            schema: tables_schema(),
            catalog_manager,
        }
    }

    fn from_table(&self, catalog: CatalogRef, schema: SchemaRef, table: TableRef) -> Row {
        let mut datums = Vec::with_capacity(self.schema.num_columns());
        datums.push(Datum::Timestamp(ENTRY_TIMESTAMP));
        datums.push(Datum::from(catalog.name()));
        datums.push(Datum::from(schema.name()));
        datums.push(Datum::from(table.name()));
        datums.push(Datum::from(table.id().as_u64()));
        datums.push(Datum::from(table.engine_type()));
        Row::from_datums(datums)
    }
}

#[async_trait]
impl<M: Manager> SystemTable for Tables<M> {
    fn name(&self) -> &str {
        TABLE_NAME
    }

    fn id(&self) -> TableId {
        TABLE_ID
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> table_engine::table::Result<SendableRecordBatchStream> {
        let catalogs = self
            .catalog_manager
            .all_catalogs()
            .map_err(|e| Box::new(e) as _)
            .context(table_engine::table::Scan { table: self.name() })?;
        let mut builder =
            RecordBatchWithKeyBuilder::new(self.schema.clone().to_record_schema_with_key());

        let projector = request
            .projected_schema
            .try_project_with_key(&self.schema)
            .expect("Should succeed to try_project_key of sys_tables");
        for catalog in &catalogs {
            for schema in &catalog
                .all_schemas()
                .map_err(|e| Box::new(e) as _)
                .context(table_engine::table::Scan { table: self.name() })?
            {
                for table in &schema
                    .all_tables()
                    .map_err(|e| Box::new(e) as _)
                    .context(table_engine::table::Scan { table: self.name() })?
                {
                    let row = self.from_table(catalog.clone(), schema.clone(), table.clone());
                    let projected_row = projector.project_row(&row, Vec::new());
                    builder
                        .append_row(projected_row)
                        .map_err(|e| Box::new(e) as _)
                        .context(table_engine::table::Scan { table: self.name() })?;
                }
            }
        }
        let record_batch = builder.build().unwrap().into_record_batch();
        Ok(Box::pin(OneRecordBatchStream {
            schema: self.schema.clone().to_record_schema(),
            record_batch: Some(record_batch),
        }))
    }
}
