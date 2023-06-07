// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned locks

use std::{
    hash::{Hash, Hasher},
    sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use common_types::hash::build_fixed_seed_ahasher;
use tokio;
/// Simple partitioned `RwLock`
pub struct PartitionedRwLock<T> {
    partitions: Vec<RwLock<T>>,
    partition_mask: usize,
}

impl<T> PartitionedRwLock<T> {
    pub fn new<F>(init_fn: F, partition_bit: usize) -> Self
    where
        F: Fn() -> T,
    {
        let partition_num = 1 << partition_bit;
        let partitions = (1..partition_num)
            .map(|_| RwLock::new(init_fn()))
            .collect::<Vec<RwLock<T>>>();
        Self {
            partitions,
            partition_mask: partition_num - 1,
        }
    }

    pub fn read<K: Eq + Hash>(&self, key: &K) -> RwLockReadGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.read().unwrap()
    }

    pub fn write<K: Eq + Hash>(&self, key: &K) -> RwLockWriteGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.write().unwrap()
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &RwLock<T> {
        let mut hasher = build_fixed_seed_ahasher();

        key.hash(&mut hasher);

        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }

    #[cfg(test)]
    fn get_partition_by_index(&self, index: usize) -> &RwLock<T> {
        &self.partitions[index]
    }
}

/// Simple partitioned `Mutex`
#[derive(Debug)]
pub struct PartitionedMutex<T> {
    partitions: Vec<Mutex<T>>,
    partition_mask: usize,
}

impl<T> PartitionedMutex<T> {
    pub fn new<F>(init_fn: F, partition_bit: usize) -> Self
    where
        F: Fn() -> T,
    {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| Mutex::new(init_fn()))
            .collect::<Vec<Mutex<T>>>();
        Self {
            partitions,
            partition_mask: partition_num - 1,
        }
    }

    pub fn lock<K: Eq + Hash>(&self, key: &K) -> MutexGuard<'_, T> {
        let mutex = self.get_partition(key);

        mutex.lock().unwrap()
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &Mutex<T> {
        let mut hasher = build_fixed_seed_ahasher();
        key.hash(&mut hasher);
        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }

    #[cfg(test)]
    fn get_partition_by_index(&self, index: usize) -> &Mutex<T> {
        &self.partitions[index]
    }

    /// This function should be marked with `#[cfg(test)]`, but there is [an issue](https://github.com/rust-lang/cargo/issues/8379) in cargo, so public this function now.
    pub fn get_all_partition(&self) -> &Vec<Mutex<T>> {
        &self.partitions
    }
}

#[derive(Debug)]
pub struct PartitionedMutexAsync<T> {
    partitions: Vec<tokio::sync::Mutex<T>>,
    partition_mask: usize,
}

impl<T> PartitionedMutexAsync<T> {
    pub fn new<F>(init_fn: F, partition_bit: usize) -> Self
    where
        F: Fn() -> T,
    {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| tokio::sync::Mutex::new(init_fn()))
            .collect::<Vec<tokio::sync::Mutex<T>>>();
        Self {
            partitions,
            partition_mask: partition_num - 1,
        }
    }

    pub async fn lock<K: Eq + Hash>(&self, key: &K) -> tokio::sync::MutexGuard<'_, T> {
        let mutex = self.get_partition(key);

        mutex.lock().await
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &tokio::sync::Mutex<T> {
        let mut hasher = build_fixed_seed_ahasher();
        key.hash(&mut hasher);
        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }

    #[cfg(test)]
    async fn get_partition_by_index(&self, index: usize) -> &tokio::sync::Mutex<T> {
        &self.partitions[index]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_partitioned_rwlock() {
        let init_hmap = HashMap::new;
        let test_locked_map = PartitionedRwLock::new(init_hmap, 4);
        let test_key = "test_key".to_string();
        let test_value = "test_value".to_string();

        {
            let mut map = test_locked_map.write(&test_key);
            map.insert(test_key.clone(), test_value.clone());
        }

        {
            let map = test_locked_map.read(&test_key);
            assert_eq!(map.get(&test_key).unwrap(), &test_value);
        }
    }

    #[test]
    fn test_partitioned_mutex() {
        let init_hmap = HashMap::new;
        let test_locked_map = PartitionedMutex::new(init_hmap, 4);
        let test_key = "test_key".to_string();
        let test_value = "test_value".to_string();

        {
            let mut map = test_locked_map.lock(&test_key);
            map.insert(test_key.clone(), test_value.clone());
        }

        {
            let map = test_locked_map.lock(&test_key);
            assert_eq!(map.get(&test_key).unwrap(), &test_value);
        }
    }

    #[tokio::test]
    async fn test_partitioned_mutex_async() {
        let init_hmap = HashMap::new;
        let test_locked_map = PartitionedMutexAsync::new(init_hmap, 4);
        let test_key = "test_key".to_string();
        let test_value = "test_value".to_string();

        {
            let mut map = test_locked_map.lock(&test_key).await;
            map.insert(test_key.clone(), test_value.clone());
        }

        {
            let map = test_locked_map.lock(&test_key).await;
            assert_eq!(map.get(&test_key).unwrap(), &test_value);
        }
    }

    #[test]
    fn test_partitioned_mutex_vis_different_partition() {
        let init_vec = Vec::<i32>::new;
        let test_locked_map = PartitionedMutex::new(init_vec, 4);
        let mutex_first = test_locked_map.get_partition_by_index(0);

        let mut _tmp_data = mutex_first.lock().unwrap();
        assert!(mutex_first.try_lock().is_err());

        let mutex_second = test_locked_map.get_partition_by_index(1);
        assert!(mutex_second.try_lock().is_ok());
        assert!(mutex_first.try_lock().is_err());
    }

    #[test]
    fn test_partitioned_rwmutex_vis_different_partition() {
        let init_vec = Vec::<i32>::new;
        let test_locked_map = PartitionedRwLock::new(init_vec, 4);
        let mutex_first = test_locked_map.get_partition_by_index(0);
        let mut _tmp = mutex_first.write().unwrap();
        assert!(mutex_first.try_write().is_err());

        let mutex_second_try_lock = test_locked_map.get_partition_by_index(1);
        assert!(mutex_second_try_lock.try_write().is_ok());
        assert!(mutex_first.try_write().is_err());
    }

    #[tokio::test]
    async fn test_partitioned_mutex_async_vis_different_partition() {
        let init_vec = Vec::<i32>::new;
        let test_locked_map = PartitionedMutexAsync::new(init_vec, 4);
        let mutex_first = test_locked_map.get_partition_by_index(0).await;

        let mut _tmp_data = mutex_first.lock().await;
        assert!(mutex_first.try_lock().is_err());

        let mutex_second = test_locked_map.get_partition_by_index(1).await;
        assert!(mutex_second.try_lock().is_ok());
        assert!(mutex_first.try_lock().is_err());
    }
}
