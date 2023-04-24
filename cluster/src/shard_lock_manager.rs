// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock as StdRwLock},
    time::{Duration, Instant},
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
        "Failed to keep alive because of expired ttl, ttl_in_resp:{ttl_in_resp}, lease_id:{lease_id}.\nBacktrace:\n{backtrace:?}"
    ))]
    KeepAliveWithExpiredTTL {
        lease_id: i64,
        ttl_in_resp: i64,
        backtrace: Backtrace,
    },

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

pub type ShardLockManagerRef = Arc<ShardLockManager>;

/// Shard lock manager is implemented based on etcd.
///
/// Only with the lock held, the shard can be operated by this node.
pub struct ShardLockManager {
    key_prefix: String,
    value: Bytes,
    lease_ttl_sec: u64,
    lock_lease_check_interval: Duration,

    etcd_client: Client,
    runtime: RuntimeRef,

    // ShardID -> ShardLock
    shard_locks: Arc<RwLock<HashMap<u32, ShardLock>>>,
}

#[derive(Debug)]
struct LeaseState {
    expired_at: Instant,
    alive: bool,
}

impl LeaseState {
    fn new() -> Self {
        Self {
            expired_at: Instant::now(),
            alive: false,
        }
    }
}

impl LeaseState {
    fn update_expired_at(&mut self, expired_at: Instant) {
        self.alive = true;
        self.expired_at = expired_at;
    }

    fn set_dead(&mut self) {
        self.alive = false;
    }
}

/// The lease of the shard lock.
struct Lease {
    /// The lease id
    id: i64,
    /// The time to live of the lease
    ttl: Duration,

    state: Arc<StdRwLock<LeaseState>>,
    keep_alive_handle: Option<JoinHandle<()>>,
}

impl Lease {
    fn new(id: i64, ttl: Duration) -> Self {
        Self {
            id,
            ttl,
            state: Arc::new(StdRwLock::new(LeaseState::new())),
            keep_alive_handle: None,
        }
    }

    /// Check whether lease is expired.
    ///
    /// If the lease is not alive, it's considered as expired.
    fn is_expired(&self) -> bool {
        let state = self.state.read().unwrap();
        if !state.alive {
            warn!(
                "Lease:{} is not alive, and is considered as expired",
                self.id
            );
            return true;
        }
        state.expired_at < Instant::now()
    }

    /// Keep alive the lease once, the state will be updated whatever the
    /// keepalive result is.
    async fn keep_alive_once(
        keeper: &mut LeaseKeeper,
        stream: &mut LeaseKeepAliveStream,
        state: &Arc<StdRwLock<LeaseState>>,
    ) -> Result<()> {
        keeper.keep_alive().await.context(KeepAlive)?;
        match stream.message().await.context(KeepAlive)? {
            Some(resp) => {
                // The ttl in the response is in seconds, let's convert it into milliseconds.
                let ttl_sec = resp.ttl();
                if ttl_sec <= 0 {
                    error!("Failed to keep lease alive because negative ttl is received, id:{}, ttl:{ttl_sec}s", keeper.id());
                    return KeepAliveWithExpiredTTL {
                        lease_id: keeper.id(),
                        ttl_in_resp: ttl_sec,
                    }
                    .fail();
                }

                let expired_at = Instant::now() + Duration::from_secs(ttl_sec as u64);
                state.write().unwrap().update_expired_at(expired_at);

                debug!(
                    "Succeed to keep lease alive, id:{}, ttl:{ttl_sec}s, expired_at:{expired_at:?}",
                    keeper.id(),
                );
                Ok(())
            }
            None => {
                error!(
                    "failed to keep lease alive because of no resp, id:{}",
                    keeper.id()
                );
                state.write().unwrap().set_dead();
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

        // Update the lease state immediately.
        Self::keep_alive_once(&mut keeper, &mut stream, &self.state).await?;

        // Send keepalive request every ttl/3.
        // FIXME: shall we ensure the interval won't be too small?
        let keep_alive_interval = self.ttl / 3;
        let lease_id = self.id;
        let state = self.state.clone();
        let handle = runtime.spawn(async move {
            loop {
                if let Err(e) = Self::keep_alive_once(&mut keeper, &mut stream, &state).await {
                    error!("Failed to keep lease alive, id:{lease_id}, err:{e}");
                    if notifier.send(Err(e)).is_err() {
                        error!("failed to send keepalive failure, lease_id:{lease_id}");
                    }

                    return
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
/// The lock is a temporary key in etcd, which is created with a lease. And the
/// lease is kept alive in background, so the lock will be expired if the lease
/// is expired.
///
/// Note: the lock will be invalid if the keepalive fails so check the validity
/// before granting the lock.
pub struct ShardLock {
    shard_id: ShardId,
    /// The temporary key in etcd
    key: Bytes,
    /// The value of the key in etcd
    value: Bytes,
    /// The lease of the lock in etcd
    ttl_sec: u64,
    /// The interval to check whether the lease is expired
    lease_check_interval: Duration,

    lease: Option<Arc<Lease>>,
    lease_check_handle: Option<JoinHandle<()>>,
    lease_keepalive_stopper: Option<oneshot::Sender<()>>,
}

impl ShardLock {
    fn new(
        shard_id: ShardId,
        key_prefix: &str,
        value: Bytes,
        ttl_sec: u64,
        expire_check_interval: Duration,
    ) -> Self {
        Self {
            shard_id,
            key: Self::lock_key(key_prefix, shard_id),
            value,
            ttl_sec,
            lease_check_interval: expire_check_interval,

            lease: None,
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
        self.wait_for_keepalive_exit().await;

        // Revoke the lease.
        if let Some(lease) = self.lease.take() {
            etcd_client
                .lease_revoke(lease.id)
                .await
                .context(RevokeLease {
                    lease_id: lease.id,
                    shard_id: self.shard_id,
                })?;
        }

        Ok(())
    }

    /// Check whether the shard lock is valid.
    ///
    /// The shard lock is valid if the associated lease is not expired.
    fn is_valid(&self) -> bool {
        self.lease
            .as_ref()
            .map(|v| !v.is_expired())
            .unwrap_or(false)
    }

    async fn wait_for_keepalive_exit(&mut self) {
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

        let lease = Arc::new(lease);
        let lease_for_check = lease.clone();
        let lock_lease_check_interval = self.lease_check_interval;
        let shard_id = self.shard_id;
        let handle = runtime.spawn(async move {
            loop {
                let timer = tokio::time::sleep(lock_lease_check_interval);
                tokio::select! {
                    _ = timer => {
                        if lease_for_check.is_expired() {
                            warn!("The lease of the shard lock is expired, shard_id:{shard_id}");
                            on_lock_expired(shard_id).await;
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
                                on_lock_expired(shard_id).await;
                            }
                            Err(_) => {
                                error!("The notifier for lease keeping alive is closed, shard_id:{shard_id}");
                                on_lock_expired(shard_id).await;
                            }
                        }
                        return
                    }
                }
            }
        });

        self.lease_check_handle = Some(handle);
        self.lease_keepalive_stopper = Some(keepalive_stop_sender);
        self.lease = Some(lease);
        Ok(())
    }
}

impl ShardLockManager {
    pub fn new(
        key_prefix: String,
        node_name: String,
        etcd_client: Client,
        lease_ttl_sec: u64,
        lock_lease_check_interval: Duration,
        runtime: RuntimeRef,
    ) -> ShardLockManager {
        let value = Bytes::from(ShardLockValue { node_name }.encode_to_vec());

        ShardLockManager {
            key_prefix,
            value,
            lease_ttl_sec,
            lock_lease_check_interval,
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
            if let Some(shard_lock) = shard_locks.get(&shard_id) {
                if shard_lock.is_valid() {
                    warn!("The lock has been granted, shard_id:{shard_id}");
                    return Ok(false);
                } else {
                    warn!(
                        "The lock is invalid even if it was granted before, and it will be granted \
                        again, shard_id:{shard_id}"
                    );
                }
            }
        }

        // Double check whether the locks has been granted.
        let mut shard_locks = self.shard_locks.write().await;
        if let Some(shard_lock) = shard_locks.get_mut(&shard_id) {
            if shard_lock.is_valid() {
                warn!("The lock has been granted, shard_id:{shard_id}");
                return Ok(false);
            }

            warn!(
                "The lock to grant is invalid even if it was granted before, and it will be granted \
                again, shard_id:{shard_id}"
            );
            let mut etcd_client = self.etcd_client.clone();
            shard_lock
                .grant(on_lock_expired, &mut etcd_client, &self.runtime)
                .await?;
        } else {
            info!("Try to grant a new shard lock, shard_id:{shard_id}");

            let mut shard_lock = ShardLock::new(
                shard_id,
                &self.key_prefix,
                self.value.clone(),
                self.lease_ttl_sec,
                self.lock_lease_check_interval,
            );

            let mut etcd_client = self.etcd_client.clone();
            shard_lock
                .grant(on_lock_expired, &mut etcd_client, &self.runtime)
                .await?;

            shard_locks.insert(shard_id, shard_lock);
        }

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
