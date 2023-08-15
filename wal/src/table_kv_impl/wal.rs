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

//! Wal based on namespace.

use std::{fmt, str, sync::Arc};

use async_trait::async_trait;
use common_types::SequenceNumber;
use generic_error::BoxError;
use log::info;
use snafu::ResultExt;
use table_kv::TableKv;

use crate::{
    log_batch::LogWriteBatch,
    manager::{
        self, error::*, BatchLogIteratorAdapter, ReadContext, ReadRequest, RegionId, ScanContext,
        ScanRequest, WalLocation, WalManager,
    },
    table_kv_impl::{
        model::NamespaceConfig,
        namespace::{Namespace, NamespaceRef},
        WalRuntimes,
    },
};

pub struct WalNamespaceImpl<T> {
    namespace: NamespaceRef<T>,
}

impl<T: TableKv> WalNamespaceImpl<T> {
    /// Open wal of namespace with given `namespace_name`, create that namespace
    /// using given `opts` if it is absent.
    pub async fn open(
        table_kv: T,
        runtimes: WalRuntimes,
        namespace_name: &str,
        config: NamespaceConfig,
    ) -> Result<WalNamespaceImpl<T>> {
        info!("Open table kv wal, namespace:{}", namespace_name);

        let namespace = Self::open_namespace(table_kv, runtimes, namespace_name, config).await?;

        let wal = WalNamespaceImpl { namespace };

        Ok(wal)
    }

    /// Open namespace, create it if not exists.
    async fn open_namespace(
        table_kv: T,
        runtimes: WalRuntimes,
        name: &str,
        config: NamespaceConfig,
    ) -> Result<NamespaceRef<T>> {
        let rt = runtimes.default_runtime.clone();
        let table_kv = table_kv.clone();
        let namespace_name = name.to_string();

        let namespace = rt
            .spawn_blocking(move || {
                Namespace::open(&table_kv, runtimes, &namespace_name, config)
                    .box_err()
                    .context(Open {
                        wal_path: namespace_name,
                    })
            })
            .await
            .box_err()
            .context(Open { wal_path: name })??;
        let namespace = Arc::new(namespace);

        Ok(namespace)
    }

    /// Close the namespace wal gracefully.
    pub async fn close_namespace(&self) -> Result<()> {
        info!(
            "Try to close namespace wal, namespace:{}",
            self.namespace.name()
        );

        self.namespace.close().await.box_err().context(Close)?;

        info!("Namespace wal closed, namespace:{}", self.namespace.name());

        Ok(())
    }
}

impl<T> fmt::Debug for WalNamespaceImpl<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalNamespaceImpl")
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl<T: TableKv> WalManager for WalNamespaceImpl<T> {
    async fn sequence_num(&self, location: WalLocation) -> Result<SequenceNumber> {
        self.namespace
            .last_sequence(location)
            .await
            .box_err()
            .context(Read)
    }

    async fn mark_delete_entries_up_to(
        &self,
        location: WalLocation,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.namespace
            .delete_entries(location, sequence_num)
            .await
            .box_err()
            .context(Delete)
    }

    async fn close_region(&self, region_id: RegionId) -> Result<()> {
        self.namespace
            .close_region(region_id)
            .await
            .box_err()
            .context(CloseRegion { region: region_id })
    }

    async fn close_gracefully(&self) -> Result<()> {
        info!(
            "Close table kv wal gracefully, namespace:{}",
            self.namespace.name()
        );

        self.close_namespace().await
    }

    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter> {
        let sync_iter = self
            .namespace
            .read_log(ctx, req)
            .await
            .box_err()
            .context(Read)?;
        let runtime = self.namespace.read_runtime().clone();

        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(sync_iter),
            runtime,
            ctx.batch_size,
        ))
    }

    async fn write(
        &self,
        ctx: &manager::WriteContext,
        batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        self.namespace
            .write_log(ctx, batch)
            .await
            .box_err()
            .context(Write)
    }

    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter> {
        let sync_iter = self
            .namespace
            .scan_log(ctx, req)
            .await
            .box_err()
            .context(Read)?;
        let runtime = self.namespace.read_runtime().clone();

        Ok(BatchLogIteratorAdapter::new_with_sync(
            Box::new(sync_iter),
            runtime,
            ctx.batch_size,
        ))
    }

    async fn get_statistics(&self) -> Option<String> {
        let stats = self.namespace.get_statistics();

        Some(stats)
    }
}
