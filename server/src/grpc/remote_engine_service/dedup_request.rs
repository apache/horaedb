// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::RwLock};

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

type Notifier = Sender<Result<RecordBatch>>;

#[derive(Debug, Default)]
pub struct Notifiers {
    notifiers: RwLock<Vec<Notifier>>,
}

impl Notifiers {
    pub fn new(notifier: Notifier) -> Self {
        let notifiers = vec![notifier];
        Self {
            notifiers: RwLock::new(notifiers),
        }
    }

    pub fn add_notifier(&self, notifier: Notifier) {
        self.notifiers.write().unwrap().push(notifier);
    }

    pub fn into_notifiers(self) -> Vec<Notifier> {
        self.notifiers.into_inner().unwrap()
    }
}

#[derive(Debug, Default)]
pub struct DedupMap {
    inner: HashMap<RequestKey, Notifiers>,
}

impl DedupMap {
    pub fn get_notifiers(&self, key: &RequestKey) -> Option<&Notifiers> {
        self.inner.get(key)
    }

    pub fn add_notifiers(&mut self, key: RequestKey, value: Notifiers) {
        self.inner.insert(key, value);
    }

    pub fn delete_notifiers(&mut self, key: RequestKey) -> Option<Notifiers> {
        self.inner.remove(&key)
    }

    pub fn contains_key(&self, key: &RequestKey) -> bool {
        self.inner.contains_key(key)
    }
}
