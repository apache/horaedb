// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use common_types::record_batch::RecordBatch;
use table_engine::{predicate::PredicateRef, table::ReadOrder};
use tokio::sync::mpsc::Sender;

use crate::grpc::remote_engine_service::Result;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequestKey {
    table: String,
    predicate: PredicateRef,
    projection: Option<Vec<usize>>,
    order: ReadOrder,
}

impl RequestKey {
    pub fn new(
        table: String,
        predicate: PredicateRef,
        projection: Option<Vec<usize>>,
        order: ReadOrder,
    ) -> Self {
        Self {
            table,
            predicate,
            projection,
            order,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Notifiers {
    txs: Vec<Sender<Result<RecordBatch>>>,
}

impl Notifiers {
    pub fn new(txs: Vec<Sender<Result<RecordBatch>>>) -> Self {
        Self { txs }
    }

    pub fn get_txs(&self) -> &Vec<Sender<Result<RecordBatch>>> {
        &self.txs
    }

    pub fn add_tx(&mut self, tx: Sender<Result<RecordBatch>>) {
        self.txs.push(tx);
    }
}

#[derive(Clone, Debug, Default)]
pub struct DedupMap {
    inner: HashMap<RequestKey, Notifiers>,
}

impl DedupMap {
    pub fn get_notifiers(&self, key: RequestKey) -> Option<&Notifiers> {
        self.inner.get(&key)
    }

    pub fn add_notifiers(&mut self, key: RequestKey, value: Notifiers) {
        self.inner.insert(key, value);
    }

    pub fn delete_notifiers(&mut self, key: &RequestKey) {
        self.inner.remove(key);
    }
}
