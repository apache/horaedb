// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Meta data of manifest.

use logger::debug;
use macros::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    manifest::meta_edit::{AddTableMeta, MetaUpdate},
    table::version::TableVersionMeta,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Apply update on non-exist table, meta update:{:?}\nBacktrace:\n{}",
        update,
        backtrace
    ))]
    TableNotFound {
        update: MetaUpdate,
        backtrace: Backtrace,
    },
}

define_result!(Error);

#[derive(Debug, Clone, PartialEq)]
pub struct MetaSnapshot {
    pub table_meta: AddTableMeta,
    pub version_meta: Option<TableVersionMeta>,
}

#[derive(Clone, Debug, Default)]
pub struct MetaSnapshotBuilder {
    table_meta: Option<AddTableMeta>,
    version_meta: Option<TableVersionMeta>,
}

impl MetaSnapshotBuilder {
    pub fn new(table_meta: Option<AddTableMeta>, version_meta: Option<TableVersionMeta>) -> Self {
        Self {
            table_meta,
            version_meta,
        }
    }

    pub fn build(mut self) -> Option<MetaSnapshot> {
        let version_meta = self.version_meta.take();
        self.table_meta.map(|v| MetaSnapshot {
            table_meta: v,
            version_meta,
        })
    }

    #[inline]
    pub fn is_table_exists(&self) -> bool {
        self.table_meta.is_some()
    }

    /// Apply the meta update.
    ///
    /// Any update except [`MetaUpdate::AddTable`] on a non-exist table will
    /// fail.
    pub fn apply_update(&mut self, update: MetaUpdate) -> Result<()> {
        debug!("Apply meta update, update:{:?}", update);

        if let MetaUpdate::AddTable(_) = &update {
        } else {
            ensure!(self.is_table_exists(), TableNotFound { update });
        }

        match update {
            MetaUpdate::AddTable(meta) => {
                self.table_meta = Some(meta);
            }
            MetaUpdate::VersionEdit(meta) => {
                let edit = meta.into_version_edit();
                let mut version = self.version_meta.take().unwrap_or_default();
                version.apply_edit(edit);
                self.version_meta = Some(version);
            }
            MetaUpdate::AlterSchema(meta) => {
                let table_meta = self.table_meta.as_mut().unwrap();
                table_meta.schema = meta.schema;
            }
            MetaUpdate::AlterOptions(meta) => {
                let table_meta = self.table_meta.as_mut().unwrap();
                table_meta.opts = meta.options;
            }
            MetaUpdate::DropTable(meta) => {
                self.table_meta = None;
                self.version_meta = None;
                debug!(
                    "Apply drop table meta update, removed table:{}",
                    meta.table_name,
                );
            }
        }

        Ok(())
    }
}
