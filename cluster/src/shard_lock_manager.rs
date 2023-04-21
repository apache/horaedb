// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use ceresdbproto::meta_event::ShardLockValue;
use common_types::{bytes::Bytes, table::ShardId};
use common_util::{
    define_result,
    runtime::{JoinHandle, RuntimeRef},
};
use etcd_client::{
    Client, Compare, CompareOp, LeaseKeepAliveStream, LeaseKeeper, PutOptions, Txn, TxnOp,
};
use log::{debug, error, info, warn};
use prost::Message;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use tokio::sync::{oneshot, RwLock};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to keep alive, err:{source}.\nBacktrace:\n{backtrace:?}"))]
    KeepAlive {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to keep alive because of no resp, lease_id:{lease_id}.\nBacktrace:\n{backtrace:?}"
    ))]
    KeepAliveWithoutResp { lease_id: i64, backtrace: Backtrace },

    #[snafu(display(
        "Failed to grant lease, shard_id:{shard_id}, err:{source}.\nBacktrace:\n{backtrace:?}"
    ))]
    GrantLease {
        shard_id: ShardId,
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create lock in etcd, shard_id:{shard_id}, err:{source}.\nBacktrace:\n{backtrace:?}"))]
    CreateLockInEtcd {
        shard_id: ShardId,
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to execute txn of create lock, shard_id:{shard_id}.\nBacktrace:\n{backtrace:?}"
    ))]
    CreateLockTxn {
        shard_id: ShardId,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to revoke the lease, lease_id:{lease_id}, shard_id:{shard_id}, err:{source}.\nBacktrace:\n{backtrace:?}"))]
    RevokeLease {
        lease_id: i64,
        shard_id: ShardId,
        source: etcd_client::Error,
        backtrace: Backtrace,
    },
}

define_result!(Error);

const DEFAULT_LEASE_SECS: u64 = 30;
const LOCK_EXPIRE_CHECK_INTERVAL: Duration = Duration::from_millis(200);

pub type ShardLockManagerRef = Arc<ShardLockManager>;

/// Shard lock manager is implemented based on etcd.
///
/// Only with the lock held, the shard can be operated by this node.
pub struct ShardLockManager {
    key_prefix: String,
    value: Bytes,
    lease_ttl_sec: u64,

    etcd_client: Client,
    runtime: RuntimeRef,

    // ShardID -> ShardLock
    shard_locks: Arc<RwLock<HashMap<u32, ShardLock>>>,
}

/// The lease of the shard lock.
struct Lease {
    /// The lease id
    id: i64,
    /// The time to live of the lease
    ttl: Duration,
    /// Expired time in milliseconds
    expired_at: Arc<AtomicU64>,

    keep_alive_handle: Option<JoinHandle<()>>,
}

impl Lease {
    fn new(id: i64, ttl: Duration) -> Self {
        Self {
            id,
            ttl,
            expired_at: Arc::new(AtomicU64::new(0)),
            keep_alive_handle: None,
        }
    }

    /// Check whether lease is expired.
    fn is_expired(&self) -> bool {
        let now = common_util::time::current_time_millis();
        let expired_at_ms = self.expired_at.load(Ordering::Relaxed);
        expired_at_ms <= now
    }

    /// Keep alive the lease once.
    ///
    /// Return a new ttl in milliseconds if succeeds.
    async fn keep_alive_once(
        keeper: &mut LeaseKeeper,
        stream: &mut LeaseKeepAliveStream,
    ) -> Result<u64> {
        keeper.keep_alive().await.context(KeepAlive)?;
        match stream.message().await.context(KeepAlive)? {
            Some(resp) => {
                // The ttl in the response is in seconds, let's convert it into milliseconds.
                let new_ttl_ms = resp.ttl() as u64 * 1000;
                let expired_at = common_util::time::current_time_millis() + new_ttl_ms;
                Ok(expired_at)
            }
            None => {
                error!(
                    "failed to keep lease alive because of no resp, id:{}",
                    keeper.id()
                );
                KeepAliveWithoutResp {
                    lease_id: keeper.id(),
                }
                .fail()
            }
        }
    }

    async fn start_keepalive(
        &mut self,
        mut stop_receiver: oneshot::Receiver<()>,
        notifier: oneshot::Sender<Result<()>>,
        etcd_client: &mut Client,
        runtime: &RuntimeRef,
    ) -> Result<()> {
        let (mut keeper, mut stream) = etcd_client
            .lease_keep_alive(self.id)
            .await
            .context(KeepAlive)?;
        let new_expired_at = Self::keep_alive_once(&mut keeper, &mut stream).await?;
        self.expired_at.store(new_expired_at, Ordering::Relaxed);

        // send keepalive request every ttl/3.
        let keep_alive_interval = self.ttl / 3;
        let lease_id = self.id;
        let expired_at = self.expired_at.clone();
        let handle = runtime.spawn(async move {
            loop {
                match Self::keep_alive_once(&mut keeper, &mut stream).await {
                    Ok(new_expired_at) => {
                        debug!("The lease {lease_id} has been kept alive, new expire time in milliseconds:{new_expired_at}");
                        expired_at.store(new_expired_at, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!("Failed to keep lease alive, id:{lease_id}, err:{e}");
                        if notifier.send(Err(e)).is_err() {
                            error!("failed to send keepalive failure, lease_id:{lease_id}");
                        }

                        return
                    }
                }
                let sleeper = tokio::time::sleep(keep_alive_interval);
                tokio::select! {
                    _ = sleeper => {
                        debug!("Try to keep the lease alive again, id:{lease_id}");
                    },
                    _ = &mut stop_receiver => {
                        info!("Stop keeping lease alive, id:{lease_id}");
                        if notifier.send(Ok(())).is_err() {
                            error!("failed to send keepalive stopping message, lease_id:{lease_id}");
                        }
                        return
                    }
                }
            }
        });

        self.keep_alive_handle = Some(handle);

        Ok(())
    }
}

/// Lock for a shard.
///
/// The lock is a temporary key in etcd, which is created with a lease.
pub struct ShardLock {
    shard_id: ShardId,
    /// The temporary key in etcd
    key: Bytes,
    /// The value of the key in etcd
    value: Bytes,
    /// The lease of the lock in etcd
    ttl_sec: u64,

    lease_id: Option<i64>,
    lease_check_handle: Option<JoinHandle<()>>,
    lease_keepalive_stopper: Option<oneshot::Sender<()>>,
}

impl ShardLock {
    fn new(shard_id: ShardId, key_prefix: &str, value: Bytes, ttl_sec: u64) -> Self {
        Self {
            shard_id,
            key: Self::lock_key(key_prefix, shard_id),
            value,
            ttl_sec,

            lease_id: None,
            lease_check_handle: None,
            lease_keepalive_stopper: None,
        }
    }

    fn lock_key(key_prefix: &str, shard_id: ShardId) -> Bytes {
        // The shard id in the key is padded with at most 20 zeros to make it sortable.
        let key = format!("{key_prefix}/{shard_id:0>20}");
        Bytes::from(key)
    }

    /// Grant the shard lock.
    ///
    /// The `on_lock_expired` callback will be called when the lock is expired,
    /// but it won't be triggered if the lock is revoked in purpose.
    async fn grant<OnExpired, Fut>(
        &mut self,
        on_lock_expired: OnExpired,
        etcd_client: &mut Client,
        runtime: &RuntimeRef,
    ) -> Result<()>
    where
        OnExpired: FnOnce(ShardId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // Grant the lease.
        let resp = etcd_client
            .lease_grant(self.ttl_sec as i64, None)
            .await
            .context(GrantLease {
                shard_id: self.shard_id,
            })?;
        let lease_id = resp.id();

        self.acquire_lock_with_lease(lease_id, etcd_client).await?;

        self.keep_lease_alive(lease_id, on_lock_expired, etcd_client, runtime)
            .await?;

        Ok(())
    }

    /// Revoke the shard lock.
    ///
    /// NOTE: the `on_lock_expired` callback set in the `grant` won't be
    /// triggered.
    async fn revoke(&mut self, etcd_client: &mut Client) -> Result<()> {
        // Stop keeping alive the lease.
        if let Some(sender) = self.lease_keepalive_stopper.take() {
            if sender.send(()).is_err() {
                warn!("Failed to stop keeping lease alive, maybe it has been stopped already so ignore it, hard_id:{}", self.shard_id);
            }
        }

        // Wait for the lease check worker to stop.
        if let Some(handle) = self.lease_check_handle.take() {
            if let Err(e) = handle.await {
                warn!("Failed to wait for the lease check worker to stop, maybe it has exited so ignore it, shard_id:{}, err:{e}", self.shard_id);
            }
        }

        // Revoke the lease.
        if let Some(lease_id) = self.lease_id.take() {
            etcd_client
                .lease_revoke(lease_id)
                .await
                .context(RevokeLease {
                    lease_id,
                    shard_id: self.shard_id,
                })?;
        }

        Ok(())
    }

    async fn acquire_lock_with_lease(&self, lease_id: i64, etcd_client: &mut Client) -> Result<()> {
        // In etcd, the version is 0 if the key does not exist.
        let not_exist = Compare::version(self.key.clone(), CompareOp::Equal, 0);
        let create_key = {
            let options = PutOptions::new().with_lease(lease_id);
            TxnOp::put(self.key.clone(), self.value.clone(), Some(options))
        };

        let create_if_not_exist = Txn::new().when([not_exist]).and_then([create_key]);

        let resp = etcd_client
            .txn(create_if_not_exist)
            .await
            .context(CreateLockInEtcd {
                shard_id: self.shard_id,
            })?;

        ensure!(
            resp.succeeded(),
            CreateLockTxn {
                shard_id: self.shard_id
            }
        );

        Ok(())
    }

    async fn keep_lease_alive<OnExpired, Fut>(
        &mut self,
        lease_id: i64,
        on_lock_expired: OnExpired,
        etcd_client: &mut Client,
        runtime: &RuntimeRef,
    ) -> Result<()>
    where
        OnExpired: FnOnce(ShardId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut lease = Lease::new(lease_id, Duration::from_secs(self.ttl_sec));
        let (lease_expire_notifier, mut lease_expire_receiver) = oneshot::channel();
        let (keepalive_stop_sender, keepalive_stop_receiver) = oneshot::channel();
        lease
            .start_keepalive(
                keepalive_stop_receiver,
                lease_expire_notifier,
                etcd_client,
                runtime,
            )
            .await?;

        let lock_expire_check_interval = LOCK_EXPIRE_CHECK_INTERVAL;
        let shard_id = self.shard_id;
        let handle = runtime.spawn(async move {
            loop {
                let timer = tokio::time::sleep(lock_expire_check_interval);
                tokio::select! {
                    _ = timer => {
                        if lease.is_expired() {
                            warn!("The lease of the shard lock is expired, shard_id:{shard_id}");
                            on_lock_expired(shard_id);
                            return
                        }
                    }
                    res = &mut lease_expire_receiver => {
                        match res {
                            Ok(Ok(_)) => {
                                info!("The lease is revoked in purpose, and no need to do anything, shard_id:{shard_id}");
                            }
                            Ok(Err(e)) => {
                                error!("The lease of the shard lock is expired, shard_id:{shard_id}, err:{e:?}");
                            }
                            Err(_) => {
                                error!("The notifier is closed and nothing to do, shard_id:{shard_id}");
                            }
                        }
                        return
                    }
                }
            }
        });

        self.lease_check_handle = Some(handle);
        self.lease_keepalive_stopper = Some(keepalive_stop_sender);
        self.lease_id = Some(lease_id);
        Ok(())
    }
}

impl ShardLockManager {
    pub fn new(
        key_prefix: String,
        node_name: String,
        etcd_client: Client,
        runtime: RuntimeRef,
    ) -> ShardLockManager {
        let value = Bytes::from(ShardLockValue { node_name }.encode_to_vec());

        ShardLockManager {
            key_prefix,
            value,
            lease_ttl_sec: DEFAULT_LEASE_SECS,
            etcd_client,
            runtime,
            shard_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Grant lock to the shard.
    ///
    /// If the lock is already granted, return false. The `on_lock_expired` will
    /// be called when the lock lease is expired, but it won't be triggered if
    /// the lock is revoked.
    pub async fn grant_lock<OnExpired, Fut>(
        &self,
        shard_id: u32,
        on_lock_expired: OnExpired,
    ) -> Result<bool>
    where
        OnExpired: FnOnce(ShardId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        info!("Try to grant lock for shard:{shard_id}");
        {
            let shard_locks = self.shard_locks.read().await;
            if shard_locks.contains_key(&shard_id) {
                warn!("The lock has been granted, shard_id:{shard_id}");
                return Ok(false);
            }
        }

        // Double check whether the locks has been granted.
        let mut shard_locks = self.shard_locks.write().await;
        if shard_locks.contains_key(&shard_id) {
            warn!("The lock has been granted, shard_id:{shard_id}");
            return Ok(false);
        }

        let mut shard_lock = ShardLock::new(
            shard_id,
            &self.key_prefix,
            self.value.clone(),
            self.lease_ttl_sec,
        );
        let mut etcd_client = self.etcd_client.clone();
        shard_lock
            .grant(on_lock_expired, &mut etcd_client, &self.runtime)
            .await?;

        shard_locks.insert(shard_id, shard_lock);

        info!("Finish granting lock for shard:{shard_id}");
        Ok(true)
    }

    /// Revoke the shard lock.
    ///
    /// If the lock is not exist, return false. And the `on_lock_expired` won't
    /// be triggered.
    pub async fn revoke_lock(&self, shard_id: u32) -> Result<bool> {
        info!("Try to revoke lock for shard:{shard_id}");

        let mut shard_locks = self.shard_locks.write().await;
        let shard_lock = shard_locks.remove(&shard_id);
        match shard_lock {
            Some(mut v) => {
                let mut etcd_client = self.etcd_client.clone();
                v.revoke(&mut etcd_client).await?;

                info!("Finish revoking lock for shard:{shard_id}");
                Ok(true)
            }
            None => {
                warn!("The lock is not exist, shard_id:{shard_id}");
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_shard_lock_key() {
        let key_prefix = "/ceresdb/defaultCluster";
        let cases = vec![
            (0, "/ceresdb/defaultCluster/00000000000000000000"),
            (10, "/ceresdb/defaultCluster/00000000000000000010"),
            (10000, "/ceresdb/defaultCluster/00000000000000010000"),
            (999999999, "/ceresdb/defaultCluster/00000000000999999999"),
        ];

        for (shard_id, expected) in cases {
            let key = ShardLock::lock_key(key_prefix, shard_id);
            assert_eq!(key, expected);
        }
    }
}
