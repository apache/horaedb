// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use bytes_ext::Bytes;
use ceresdbproto::meta_event::ShardLockValue;
use common_types::table::ShardId;
use etcd_client::{
    Client, Compare, CompareOp, LeaseKeepAliveStream, LeaseKeeper, PutOptions, Txn, TxnOp,
};
use logger::{debug, error, info, warn};
use macros::define_result;
use prost::Message;
use runtime::{JoinHandle, RuntimeRef};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use tokio::sync::{oneshot, RwLock as AsyncRwLock};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Failed to keep alive, lease_id:{lease_id}, err:{source}.\nBacktrace:\n{backtrace:?}"
    ))]
    KeepAlive {
        lease_id: i64,
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
        "Try to start multiple keepalive procedure in background, lease_id:{lease_id}, err:{source}.\nBacktrace:\n{backtrace:?}"
    ))]
    ConcurrentKeepalive {
        source: tokio::sync::TryLockError,
        lease_id: i64,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get lease, shard_id:{shard_id}, lease_id:{lease_id}, err:{source}.\nBacktrace:\n{backtrace:?}"
    ))]
    GetLease {
        shard_id: ShardId,
        lease_id: i64,
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to get lock key, shard_id:{shard_id}, err:{source}.\nBacktrace:\n{backtrace:?}"
    ))]
    GetLockKey {
        shard_id: ShardId,
        source: etcd_client::Error,
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

    #[snafu(display(
        "Failed to grant lease, shard_id:{shard_id}, ttl_sec:{ttl_sec}.\nBacktrace:\n{backtrace:?}"
    ))]
    GrantLeaseWithInvalidTTL {
        shard_id: ShardId,
        ttl_sec: i64,
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
    lock_key_prefix: String,
    lock_value: Bytes,
    lock_lease_ttl_sec: u64,
    lock_lease_check_interval: Duration,
    enable_fast_reacquire_lock: bool,
    rpc_timeout: Duration,

    etcd_client: Client,
    runtime: RuntimeRef,

    // ShardID -> ShardLock
    shard_locks: Arc<AsyncRwLock<HashMap<u32, ShardLock>>>,
}

#[derive(Debug)]
struct LeaseState {
    expired_at: Instant,
}

impl LeaseState {
    fn new(expired_at: Instant) -> Self {
        Self { expired_at }
    }

    /// Get the duration until the lease is expired.
    ///
    /// The actual duration will be still returned even if the lease is not
    /// alive. And None will be returned if the lease is expired.
    fn duration_until_expired(&self) -> Option<Duration> {
        let expired_at = self.expired_at;
        let now = Instant::now();
        expired_at.checked_duration_since(now)
    }

    /// Check whether lease is expired.
    fn is_expired(&self) -> bool {
        self.expired_at < Instant::now()
    }
}

impl LeaseState {
    fn update_expired_at(&mut self, expired_at: Instant) {
        self.expired_at = expired_at;
    }
}

#[derive(Debug)]
enum KeepAliveStopReason {
    Failure {
        err: Error,
        stop_rx: oneshot::Receiver<()>,
    },
    Exit,
}

#[derive(Debug)]
enum KeepAliveResult {
    Ok,
    Failure {
        err: Error,
        stop_rx: oneshot::Receiver<()>,
    },
}

/// The lease of the shard lock.
struct Lease {
    /// The lease id
    id: i64,
    /// The time to live of the lease
    ttl: Duration,

    state: Arc<RwLock<LeaseState>>,
    /// The handle for the keep alive task in background.
    ///
    /// TODO: shall we wait for it to exit?
    keep_alive_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Lease {
    fn new(id: i64, ttl: Duration, initial_state: LeaseState) -> Self {
        Self {
            id,
            ttl,
            state: Arc::new(RwLock::new(initial_state)),
            keep_alive_handle: Mutex::new(None),
        }
    }

    fn is_expired(&self) -> bool {
        let state = self.state.read().unwrap();
        state.is_expired()
    }

    fn duration_until_expired(&self) -> Option<Duration> {
        let state = self.state.read().unwrap();
        state.duration_until_expired()
    }

    /// Keep alive the lease once, the state will be updated whatever the
    /// keepalive result is.
    async fn keep_alive_once(
        keeper: &mut LeaseKeeper,
        stream: &mut LeaseKeepAliveStream,
        state: &Arc<RwLock<LeaseState>>,
    ) -> Result<()> {
        keeper.keep_alive().await.context(KeepAlive {
            lease_id: keeper.id(),
        })?;
        match stream.message().await.context(KeepAlive {
            lease_id: keeper.id(),
        })? {
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
                KeepAliveWithoutResp {
                    lease_id: keeper.id(),
                }
                .fail()
            }
        }
    }

    async fn start_keepalive(
        &self,
        mut stop_rx: oneshot::Receiver<()>,
        notifier: oneshot::Sender<KeepAliveStopReason>,
        etcd_client: &mut Client,
        runtime: &RuntimeRef,
    ) -> KeepAliveResult {
        let (mut keeper, mut stream) = match etcd_client
            .lease_keep_alive(self.id)
            .await
            .context(KeepAlive { lease_id: self.id })
        {
            Ok((keeper, stream)) => (keeper, stream),
            Err(err) => {
                return KeepAliveResult::Failure { err, stop_rx };
            }
        };

        // Update the lease state immediately.
        if let Err(err) = Self::keep_alive_once(&mut keeper, &mut stream, &self.state).await {
            return KeepAliveResult::Failure { err, stop_rx };
        }

        // Send keepalive request every ttl/3.
        // FIXME: shall we ensure the interval won't be too small?
        let keep_alive_interval = self.ttl / 3;
        let lease_id = self.id;
        let state = self.state.clone();
        let handle = runtime.spawn(async move {
            loop {
                if let Err(err) = Self::keep_alive_once(&mut keeper, &mut stream, &state).await {
                    error!("Failed to keep lease alive, id:{lease_id}, err:{err}");
                    let reason = KeepAliveStopReason::Failure {
                        err,
                        stop_rx,
                    };
                    if notifier.send(reason).is_err() {
                        error!("Failed to send keepalive failure, lease_id:{lease_id}");
                    }

                    return
                }
                let sleeper = tokio::time::sleep(keep_alive_interval);
                tokio::select! {
                    _ = sleeper => {
                        debug!("Try to keep the lease alive again, id:{lease_id}");
                    },
                    _ = &mut stop_rx => {
                        info!("Stop keeping lease alive, id:{lease_id}");
                        if notifier.send(KeepAliveStopReason::Exit).is_err() {
                            error!("Failed to send keepalive stopping message, lease_id:{lease_id}");
                        }
                        return
                    }
                }
            }
        });

        *self.keep_alive_handle.lock().unwrap() = Some(handle);

        KeepAliveResult::Ok
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
    /// The previous lease will be re-used if set
    enable_fast_reacquire: bool,
    /// The timeout for etcd rpc
    rpc_timeout: Duration,

    lease: Option<Arc<Lease>>,
    lease_check_handle: Option<JoinHandle<()>>,
    lease_keepalive_stopper: Option<oneshot::Sender<()>>,
}

/// The information about an etcd lease.
#[derive(Debug, Clone)]
struct LeaseInfo {
    id: i64,
    expired_at: Instant,
}

impl ShardLock {
    fn new(
        shard_id: ShardId,
        key_prefix: &str,
        value: Bytes,
        ttl_sec: u64,
        lease_check_interval: Duration,
        enable_fast_reacquire: bool,
        rpc_timeout: Duration,
    ) -> Self {
        Self {
            shard_id,
            key: Self::lock_key(key_prefix, shard_id),
            value,
            ttl_sec,
            lease_check_interval,
            enable_fast_reacquire,
            rpc_timeout,

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

    /// The shard lock is allowed to acquired in a fast way if the lock has been
    /// created by this node just before.
    ///
    /// The slow way has to wait for the previous lock lease expired. And the
    /// fast way is to reuse the lease if it is not expired and the lock's value
    /// is the same.
    async fn maybe_fast_acquire_lock(&self, etcd_client: &mut Client) -> Result<Option<LeaseInfo>> {
        if !self.enable_fast_reacquire {
            return Ok(None);
        }

        let resp = etcd_client
            .get(self.key.clone(), None)
            .await
            .context(GetLockKey {
                shard_id: self.shard_id,
            })?;
        if resp.kvs().len() != 1 {
            warn!(
                "Expect exactly one key value pair, but found {} kv pairs, shard_id:{}",
                resp.kvs().len(),
                self.shard_id
            );
            return Ok(None);
        }
        let lease_id = resp.kvs()[0].lease();
        if lease_id == 0 {
            // There is no lease attached to the lock key.
            return Ok(None);
        }

        // FIXME: A better way is to compare the specific field of the decoded values.
        if resp.kvs()[0].value() != self.value {
            warn!(
                "Try to acquire a lock held by others, shard_id:{}",
                self.shard_id
            );
            return Ok(None);
        }

        let ttl_sec = etcd_client
            .lease_time_to_live(lease_id, None)
            .await
            .context(GetLease {
                shard_id: self.shard_id,
                lease_id,
            })?
            .ttl();

        if ttl_sec == 0 {
            // The lease has expired.
            return Ok(None);
        }

        let lease_expired_at = Instant::now() + Duration::from_secs(ttl_sec as u64);
        Ok(Some(LeaseInfo {
            id: lease_id,
            expired_at: lease_expired_at,
        }))
    }

    async fn slow_acquire_lock(&self, etcd_client: &mut Client) -> Result<LeaseInfo> {
        // Grant the lease first.
        let resp = etcd_client
            .lease_grant(self.ttl_sec as i64, None)
            .await
            .context(GrantLease {
                shard_id: self.shard_id,
            })?;
        ensure!(
            resp.ttl() > 0,
            GrantLeaseWithInvalidTTL {
                shard_id: self.shard_id,
                ttl_sec: resp.ttl()
            }
        );

        let lease_expired_at = Instant::now() + Duration::from_secs(resp.ttl() as u64);
        let lease_id = resp.id();
        self.create_lock_with_lease(lease_id, etcd_client).await?;

        Ok(LeaseInfo {
            id: lease_id,
            expired_at: lease_expired_at,
        })
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
    ) -> Result<bool>
    where
        OnExpired: FnOnce(ShardId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.is_valid() {
            // FIXME: maybe the lock will be expired soon.
            warn!("The lock is already granted, shard_id:{}", self.shard_id);
            return Ok(false);
        }

        let lease_info = match self.maybe_fast_acquire_lock(etcd_client).await {
            Ok(Some(v)) => {
                info!("Shard lock is acquired fast, shard_id:{}", self.shard_id);
                v
            }
            Ok(None) => {
                warn!(
                    "No lock to reuse, try to slow acquire lock, shard_id:{}",
                    self.shard_id
                );
                self.slow_acquire_lock(etcd_client).await?
            }
            Err(e) => {
                warn!(
                    "Failed to fast acquire lock, try to slow acquire lock, shard_id:{}, err:{e}",
                    self.shard_id
                );
                self.slow_acquire_lock(etcd_client).await?
            }
        };

        self.keep_lease_alive(
            lease_info.id,
            lease_info.expired_at,
            on_lock_expired,
            etcd_client,
            runtime,
        )
        .await?;

        Ok(true)
    }

    /// Revoke the shard lock.
    ///
    /// NOTE: the `on_lock_expired` callback set in the `grant` won't be
    /// triggered.
    async fn revoke(&mut self, etcd_client: &mut Client) -> Result<()> {
        self.stop_keepalive().await;

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

    async fn stop_keepalive(&mut self) {
        info!(
            "Wait for background keepalive exit, shard_id:{}",
            self.shard_id
        );

        // Stop keeping alive the lease.
        if let Some(sender) = self.lease_keepalive_stopper.take() {
            if sender.send(()).is_err() {
                warn!("Failed to stop keeping lease alive, maybe it has been stopped already so ignore it, hard_id:{}", self.shard_id);
            }
        }

        // Wait for the lease check worker to stop.
        if let Some(handle) = self.lease_check_handle.take() {
            handle.abort();
        }

        info!(
            "Finish exiting from background keepalive task, shard_id:{}",
            self.shard_id
        );
    }

    async fn create_lock_with_lease(&self, lease_id: i64, etcd_client: &mut Client) -> Result<()> {
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

    /// Keep alive the lease.
    ///
    /// The `on_lock_expired` callback will be called when the lock is expired,
    /// but it won't be triggered if the lock is revoked in purpose.
    /// The keepalive procedure supports retrying if the failure is caused by
    /// rpc error, and it will stop if the lease is expired.
    async fn keep_lease_alive<OnExpired, Fut>(
        &mut self,
        lease_id: i64,
        expired_at: Instant,
        on_lock_expired: OnExpired,
        etcd_client: &Client,
        runtime: &RuntimeRef,
    ) -> Result<()>
    where
        OnExpired: FnOnce(ShardId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // Try to acquire the lock to ensure there is only one running keepalive
        // procedure.
        let initial_state = LeaseState::new(expired_at);
        let lease = Arc::new(Lease::new(
            lease_id,
            Duration::from_secs(self.ttl_sec),
            initial_state,
        ));

        let shard_id = self.shard_id;
        let rpc_timeout = self.rpc_timeout;
        let lock_lease_check_interval = self.lease_check_interval;
        let (keepalive_stop_sender, keepalive_stop_rx) = oneshot::channel();
        let mut etcd_client = etcd_client.clone();
        let runtime_for_bg = runtime.clone();
        let lease_for_bg = lease.clone();
        let handle = runtime.spawn(async move {
            // This receiver is used to stop the underlying keepalive procedure.
            let mut keepalive_stop_rx = keepalive_stop_rx;
            // The loop is for retrying the keepalive procedure if the failure results from rpc error.
            loop {
                if let Some(dur_until_expired)  = lease_for_bg.duration_until_expired() {
                    // Don't start next keep alive immediately, wait for a while to avoid too many requests.
                    // The wait time is calculated based on the lease ttl and the rpc timeout. And if the time before
                    // the lease expired is not enough (less than 1.5*rpc timeout), the keepalive will not be scheduled
                    // and the lock is considered expired.
                    let keepalive_cost = rpc_timeout + rpc_timeout / 2;
                    if let Some(wait) = dur_until_expired.checked_sub(keepalive_cost) {
                        info!(
                            "Next keepalive will be schedule after {wait:?}, shard_id:{shard_id}, lease_id:{lease_id}, \
                            keepalive_cost:{keepalive_cost:?}, duration_until_expired:{dur_until_expired:?}"
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {
                                info!("Start to keepalive, shard_id:{shard_id}, lease_id:{lease_id}");
                            }
                            _ = &mut keepalive_stop_rx => {
                                info!("Recv signal to stop keepalive, shard_id:{shard_id}, lease_id:{lease_id}");
                                return;
                            }
                        }

                    } else {
                        warn!(
                            "The lease is about to expire, and trigger the on_lock_expire callback, shard_id:{shard_id}, \
                             lease_id:{lease_id}"
                        );

                        on_lock_expired(shard_id).await;
                        return;
                    }
                }

                let (lease_expire_notifier, mut lease_expire_rx) = oneshot::channel();
                if let KeepAliveResult::Failure { err, stop_rx } = lease_for_bg
                    .start_keepalive(
                        keepalive_stop_rx,
                        lease_expire_notifier,
                        &mut etcd_client,
                        &runtime_for_bg,
                    )
                    .await {
                        error!("Failed to start keepalive and will retry , shard_id:{shard_id}, err:{err}");
                        keepalive_stop_rx = stop_rx;
                        continue;
                    }

                // Start to keep the lease alive forever.
                // If the lease is expired, the whole task will be stopped. However, if there are some failure in the
                // keepalive procedure, we will retry.
                'outer: loop {
                    let timer = tokio::time::sleep(lock_lease_check_interval);
                    tokio::select! {
                        _ = timer => {
                            if lease_for_bg.is_expired() {
                                warn!("The lease of the shard lock is expired, shard_id:{shard_id}");
                                on_lock_expired(shard_id).await;
                                return
                            }
                        }
                        res = &mut lease_expire_rx => {
                            match res {
                                Ok(reason) => match reason {
                                    KeepAliveStopReason::Exit => {
                                        info!("The lease is revoked in purpose, and no need to do anything, shard_id:{shard_id}");
                                        return;
                                    }
                                    KeepAliveStopReason::Failure { err, stop_rx } => {
                                        error!("Fail to keep lease alive, and will retry, shard_id:{shard_id}, err:{err:?}");
                                        keepalive_stop_rx = stop_rx;
                                        // Break the outer loop to avoid the macro may introduce a new loop.
                                        break 'outer;
                                    }
                                }
                                Err(_) => {
                                    // Unreachable! Because the notifier will always send a value before it is closed.
                                    error!("The notifier for lease keeping alive is closed, will trigger callback, shard_id:{shard_id}");
                                    on_lock_expired(shard_id).await;
                                    return;
                                }
                            }
                        }
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

#[derive(Clone, Debug)]
pub struct Config {
    pub node_name: String,
    pub lock_key_prefix: String,
    pub lock_lease_ttl_sec: u64,
    pub lock_lease_check_interval: Duration,
    pub enable_fast_reacquire_lock: bool,
    pub rpc_timeout: Duration,
    pub runtime: RuntimeRef,
}

impl ShardLockManager {
    pub fn new(config: Config, etcd_client: Client) -> ShardLockManager {
        let Config {
            node_name,
            lock_key_prefix,
            lock_lease_ttl_sec,
            lock_lease_check_interval,
            enable_fast_reacquire_lock,
            rpc_timeout,
            runtime,
        } = config;

        let value = Bytes::from(ShardLockValue { node_name }.encode_to_vec());

        ShardLockManager {
            lock_key_prefix,
            lock_value: value,
            lock_lease_ttl_sec,
            lock_lease_check_interval,
            rpc_timeout,
            enable_fast_reacquire_lock,
            etcd_client,
            runtime,
            shard_locks: Arc::new(AsyncRwLock::new(HashMap::new())),
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
        info!("Try to grant lock for shard, shard_id:{shard_id}");

        let mut shard_locks = self.shard_locks.write().await;
        if let Some(shard_lock) = shard_locks.get_mut(&shard_id) {
            let mut etcd_client = self.etcd_client.clone();
            warn!("The shard lock was created before, and grant it again now, shard_id:{shard_id}");
            shard_lock
                .grant(on_lock_expired, &mut etcd_client, &self.runtime)
                .await?;
        } else {
            info!("Try to grant a new shard lock, shard_id:{shard_id}");

            let mut shard_lock = ShardLock::new(
                shard_id,
                &self.lock_key_prefix,
                self.lock_value.clone(),
                self.lock_lease_ttl_sec,
                self.lock_lease_check_interval,
                self.enable_fast_reacquire_lock,
                self.rpc_timeout,
            );

            let mut etcd_client = self.etcd_client.clone();
            shard_lock
                .grant(on_lock_expired, &mut etcd_client, &self.runtime)
                .await?;

            shard_locks.insert(shard_id, shard_lock);
        }

        info!("Finish granting lock for shard, shard_id:{shard_id}");
        Ok(true)
    }

    /// Revoke the shard lock.
    ///
    /// If the lock is not exist, return false. And the `on_lock_expired` won't
    /// be triggered.
    pub async fn revoke_lock(&self, shard_id: u32) -> Result<bool> {
        info!("Try to revoke lock for shard, shard_id:{shard_id}");

        let mut shard_locks = self.shard_locks.write().await;
        let shard_lock = shard_locks.remove(&shard_id);
        let res = match shard_lock {
            Some(mut v) => {
                let mut etcd_client = self.etcd_client.clone();
                v.revoke(&mut etcd_client).await?;

                info!("Finish revoking lock for shard, shard_id:{shard_id}");
                Ok(true)
            }
            None => {
                warn!("The lock is not exist, shard_id:{shard_id}");
                Ok(false)
            }
        };

        info!("Finish revoke lock for shard, shard_id:{shard_id}");
        res
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
