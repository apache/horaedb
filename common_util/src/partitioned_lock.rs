use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    sync::Arc,
};

use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Simple partitioned `RwLock`
pub struct PartitionedRwLock<T> {
    partitions: Vec<Arc<RwLock<T>>>,
}

impl<T> PartitionedRwLock<T> {
    // TODO: we should get the nearest 2^n of `partition_num` as real
    // `partition_num`. By doing so, we can use "&" to get partition rather than
    // "%".
    pub fn new(t: T, partition_num: NonZeroUsize) -> Self {
        let partition_num = partition_num.get();
        let locked_content = Arc::new(RwLock::new(t));
        Self {
            partitions: vec![locked_content; partition_num],
        }
    }

    pub async fn read<K: Eq + Hash>(&self, key: &K) -> RwLockReadGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.read().await
    }

    pub async fn write<K: Eq + Hash>(&self, key: &K) -> RwLockWriteGuard<'_, T> {
        let rwlock = self.get_partition(key);

        rwlock.write().await
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &RwLock<T> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let partition_num = self.partitions.len();

        &self.partitions[(hasher.finish() as usize) % partition_num]
    }
}

/// Simple partitioned `Mutex`
pub struct PartitionedMutex<T> {
    partitions: Vec<Arc<Mutex<T>>>,
}

impl<T> PartitionedMutex<T> {
    // TODO: we should get the nearest 2^n of `partition_num` as real
    // `partition_num`. By doing so, we can use "&" to get partition rather than
    // "%".
    pub fn new(t: T, partition_num: NonZeroUsize) -> Self {
        let partition_num = partition_num.get();
        let locked_content = Arc::new(Mutex::new(t));
        Self {
            partitions: vec![locked_content; partition_num],
        }
    }

    pub async fn lock<K: Eq + Hash>(&self, key: &K) -> MutexGuard<'_, T> {
        let mutex = self.get_partition(key);

        mutex.lock().await
    }

    fn get_partition<K: Eq + Hash>(&self, key: &K) -> &Mutex<T> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let partition_num = self.partitions.len();

        &self.partitions[(hasher.finish() as usize) % partition_num]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_partitioned_rwlock() {
        let test_locked_map =
            PartitionedRwLock::new(HashMap::new(), NonZeroUsize::new(10).unwrap());
        let test_key = "test_key".to_string();
        let test_value = "test_value".to_string();

        {
            let mut map = test_locked_map.write(&test_key).await;
            map.insert(test_key.clone(), test_value.clone());
        }

        {
            let map = test_locked_map.read(&test_key).await;
            assert_eq!(map.get(&test_key).unwrap(), &test_value);
        }
    }

    #[tokio::test]
    async fn test_partitioned_mutex() {
        let test_locked_map = PartitionedMutex::new(HashMap::new(), NonZeroUsize::new(10).unwrap());
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
}
