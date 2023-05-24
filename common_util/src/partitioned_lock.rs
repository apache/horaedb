// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partitioned locks

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// Simple partitioned `RwLock`
pub struct PartitionedRwLock<T> {
    partitions: Vec<RwLock<T>>,
    partition_mask: usize,
}

impl<T> PartitionedRwLock<T>
where
    T: Clone,
{
    pub fn new(t: T, partition_bit: usize) -> Self {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| RwLock::new(t.clone()))
            .collect::<Vec<_>>();
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
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }
}

/// Simple partitioned `Mutex`
pub struct PartitionedMutex<T> {
    partitions: Vec<Mutex<T>>,
    partition_mask: usize,
}

impl<T> PartitionedMutex<T>
where
    T: Clone,
{
    pub fn new(t: T, partition_bit: usize) -> Self {
        let partition_num = 1 << partition_bit;
        let partitions = (0..partition_num)
            .map(|_| Mutex::new(t.clone()))
            .collect::<Vec<_>>();
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
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        &self.partitions[(hasher.finish() as usize) & self.partition_mask]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_partitioned_rwlock() {
        let test_locked_map = PartitionedRwLock::new(HashMap::new(), 4);
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
        let test_locked_map = PartitionedMutex::new(HashMap::new(), 4);
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

    #[test]
    fn test_partitioned_mutex_vis_different_partition() {
        let tmp_vec: Vec<f32> = Vec::new();
        let test_locked_map = PartitionedMutex::new(tmp_vec, 4);
        let test_key_first = "test_key_first".to_string();
        let mutex_first = test_locked_map.get_partition(&test_key_first);
        let mut _tmp_data = mutex_first.lock().unwrap();
        assert!(mutex_first.try_lock().is_err());

        let test_key_second = "test_key_second".to_string();
        let mutex_second = test_locked_map.get_partition(&test_key_second);
        assert!(mutex_second.try_lock().is_ok());
        assert!(mutex_first.try_lock().is_err());
    }

    #[test]
    fn test_partitioned_rwmutex_vis_different_partition() {
        let tmp_vec: Vec<f32> = Vec::new();
        let test_locked_map = PartitionedRwLock::new(tmp_vec, 4);
        let test_key_first = "test_key_first".to_string();
        let mutex_first = test_locked_map.get_partition(&test_key_first);
        let mut _tmp = mutex_first.write().unwrap();
        assert!(mutex_first.try_write().is_err());

        let test_key_second = "test_key_second".to_string();
        let mutex_second_try_lock = test_locked_map.get_partition(&test_key_second);
        assert!(mutex_second_try_lock.try_write().is_ok());
        assert!(mutex_first.try_write().is_err());
    }
}
