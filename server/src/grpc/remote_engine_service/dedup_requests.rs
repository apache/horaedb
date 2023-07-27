// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, hash::Hash, sync::RwLock};

use tokio::sync::mpsc::Sender;

type Notifier<T, E> = Sender<Result<T, E>>;

#[derive(Debug)]
struct Notifiers<T, E> {
    notifiers: RwLock<Vec<Notifier<T, E>>>,
}

impl<T, E> Notifiers<T, E> {
    pub fn new(notifier: Notifier<T, E>) -> Self {
        let notifiers = vec![notifier];
        Self {
            notifiers: RwLock::new(notifiers),
        }
    }

    pub fn add_notifier(&self, notifier: Notifier<T, E>) {
        self.notifiers.write().unwrap().push(notifier);
    }
}

#[derive(Debug)]
pub struct RequestNotifiers<K, T, E>
where
    K: PartialEq + Eq + Hash,
{
    inner: RwLock<HashMap<K, Notifiers<T, E>>>,
}

impl<K, T, E> Default for RequestNotifiers<K, T, E>
where
    K: PartialEq + Eq + Hash,
{
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl<K, T, E> RequestNotifiers<K, T, E>
where
    K: PartialEq + Eq + Hash,
{
    /// Insert a notifier for the given key.
    pub fn insert_notifier(&self, key: K, notifier: Notifier<T, E>) -> RequestResult {
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
    pub fn take_notifiers(&self, key: &K) -> Option<Vec<Notifier<T, E>>> {
        self.inner
            .write()
            .unwrap()
            .remove(key)
            .map(|notifiers| notifiers.notifiers.into_inner().unwrap())
    }
}

pub enum RequestResult {
    // The first request for this key, need to handle this request.
    First,
    // There are other requests for this key, just wait for the result.
    Wait,
}
