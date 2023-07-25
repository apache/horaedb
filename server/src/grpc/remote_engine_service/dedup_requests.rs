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
struct Notifiers {
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
}

#[derive(Debug, Default)]
pub struct RequestNotifiers {
    inner: RwLock<HashMap<RequestKey, Notifiers>>,
}

impl RequestNotifiers {
    /// Insert a notifier for the given key.
    pub fn insert_notifier(&self, key: RequestKey, notifier: Notifier) -> RequestResult {
        // First try to read the notifiers, if the key exists, add the notifier to the
        // notifiers.
        let notifiers = self.inner.read().unwrap();
        if notifiers.contains_key(&key) {
            notifiers.get(&key).unwrap().add_notifier(notifier);
            return RequestResult::Wait;
        }
        drop(notifiers);

        // If the key does not exist, try to write the notifiers.
        let mut notifiers = self.inner.write().unwrap();
        // double check, if the key exists, add the notifier to the notifiers.
        if notifiers.contains_key(&key) {
            notifiers.get(&key).unwrap().add_notifier(notifier);
            return RequestResult::Wait;
        }

        //the key is not existed, insert the key and the notifier.
        notifiers.insert(key, Notifiers::new(notifier));
        RequestResult::First
    }

    /// Take the notifiers for the given key, and remove the key from the map.
    pub fn take_notifiers(&self, key: RequestKey) -> Option<Vec<Notifier>> {
        self.inner
            .write()
            .unwrap()
            .remove(&key)
            .map(|notifiers| notifiers.notifiers.into_inner().unwrap())
    }
}

pub enum RequestResult {
    // The first request for this key, need to handle this request.
    First,
    // There are other requests for this key, just wait for the result.
    Wait,
}
