// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use prom_remote_api::types::{ReadRequest, ReadResponse, RemoteStorage, Result, WriteRequest};
use query_engine::executor::Executor as QueryExecutor;

use crate::instance::InstanceRef;

pub struct CeresDBStorage<Q: QueryExecutor + 'static> {
    #[allow(dead_code)]
    instance: InstanceRef<Q>,
}

impl<Q: QueryExecutor + 'static> CeresDBStorage<Q> {
    pub fn new(instance: InstanceRef<Q>) -> Self {
        Self { instance }
    }
}

#[async_trait]
impl<Q: QueryExecutor + 'static> RemoteStorage for CeresDBStorage<Q> {
    /// Write samples to remote storage
    async fn write(&self, req: WriteRequest) -> Result<()> {
        println!("mock write, req:{req:?}");

        Ok(())
    }

    /// Read samples from remote storage
    async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        println!("mock query, req:{req:?}");

        Ok(ReadResponse::default())
    }
}
