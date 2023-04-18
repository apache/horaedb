// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration, vec};

use ceresdbproto::meta_event::ShardLockValue;
use common_util::error::BoxError;
use etcd_client::*;
use log::{error, info};
use prost::Message;
use snafu::ResultExt;
use tokio::{
    sync::{
        oneshot,
        oneshot::{error::TryRecvError, Sender},
        RwLock,
    },
    time::Interval,
};

use crate::{Internal, Result};

const SHARD_LEADER_PATH: &str = "shards";
const DEFAULT_LEASE_SECS: i64 = 30;
const KEEP_ALIVE_PERIOD_SECS: u64 = 5;

pub struct ShardLockManager {
    cluster_name: String,
    endpoint: String,
    root_path: String,
    shard_leader_key: String,

    etcd_cli: Client,
    lease_ttl_sec: i64,

    // ShardID -> ShardLock
    shard_lock_map: Arc<RwLock<HashMap<u32, RwLock<ShardLock>>>>,
}

pub struct ShardLock {
    shard_id: u32,
    lock_ttl_sec: i64,
    shard_lock_path: String,
    // Deleted callback contains expired delete & invoke delete
    lock_deleted_callback: Arc<dyn Fn(u32) -> Result<bool> + Send + Sync + 'static>,

    etcd_cli: Client,
    endpoint: String,

    tx: Option<Sender<()>>,
}

impl ShardLock {
    async fn grant_lock(&mut self) -> Result<bool> {
        let check_result = self.check_lock_owner().await;
        match check_result {
            Ok(result) => {
                match result {
                    None => {
                        // Do nothing and continue grant
                    }
                    Some(result) => return Ok(result),
                }
            }
            Err(e) => return Err(e),
        }

        // Get lock, put data to etcd with lease.
        let lease_id = self.generate_lease().await?;

        self.update_etcd_txn(lease_id).await?;

        let keep_alive_interval =
            tokio::time::interval(Duration::from_secs(KEEP_ALIVE_PERIOD_SECS));

        self.keep_lease(lease_id, keep_alive_interval);

        Ok(true)
    }

    fn keep_lease(&mut self, lease_id: i64, mut keep_alive_interval: Interval) {
        let mut client = self.etcd_cli.clone();
        let shard_id = self.shard_id;
        let lock_deleted_callback = self.lock_deleted_callback.clone();
        let (tx, mut rx) = oneshot::channel::<()>();
        self.tx = Some(tx);
        tokio::spawn(async move {
            let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await.unwrap();
            loop {
                keep_alive_interval.tick().await;

                let mut lease_failed_flag = false;

                match rx.try_recv() {
                    Ok(_) => {
                        info!("receive close request");
                        return;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {}
                        TryRecvError::Closed => {
                            error!("shardID {},receive closed,{}", shard_id, e)
                        }
                    },
                }

                let resp = keeper.keep_alive().await;
                match resp {
                    Ok(_) => {}
                    Err(e) => {
                        error!("keep alive failed, shard will be removed, err msg {}", e);
                        lease_failed_flag = true
                    }
                }

                match stream.message().await {
                    Ok(op) => {
                        match op {
                            None => {
                                error!("keep alive failed, connection is closed, shard will be removed");
                                lease_failed_flag = true
                            }
                            Some(resp) => {
                                info!(
                                    "shard {},lease {:?} keep alive, new ttl {:?}",
                                    shard_id,
                                    resp.id(),
                                    resp.ttl()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // Close Shard
                        error!("keep alive failed, shard will be removed, err msg {}", e);
                        lease_failed_flag = true
                    }
                }

                if lease_failed_flag {
                    // TODO: Consider how to property close shard when lease is expired.
                    let callback_result = lock_deleted_callback(shard_id);
                    info!("invoke callback, result {:?}", callback_result);
                    return;
                }
            }
        });
    }

    async fn update_etcd_txn(&mut self, lease_id: i64) -> Result<TxnResponse> {
        self.etcd_cli
            .clone()
            .txn(
                Txn::new()
                    .when(vec![Compare::version(
                        self.shard_lock_path.clone(),
                        CompareOp::Equal,
                        0,
                    )])
                    .and_then(vec![TxnOp::put(
                        self.shard_lock_path.clone(),
                        ShardLockValue {
                            node_name: self.endpoint.clone(),
                        }
                        .encode_to_vec(),
                        Some(PutOptions::new().with_lease(lease_id).with_prev_key()),
                    )]),
            )
            .await
            .box_err()
            .context(Internal {
                msg: String::from("etcd client txn failed"),
            })
    }

    async fn generate_lease(&mut self) -> Result<i64> {
        let lease_resp = self
            .etcd_cli
            .clone()
            .lease_grant(self.lock_ttl_sec, None)
            .await
            .box_err()
            .context(Internal {
                msg: String::from("etcd client lease grant failed"),
            });

        match lease_resp {
            Ok(resp) => Ok(resp.id()),
            Err(e) => Err(e),
        }
    }

    // 1. No lock exists
    // 2. Lock exists, owner is current node
    // 3. Lock exists, owner is not current node
    async fn check_lock_owner(&mut self) -> Result<Option<bool>> {
        // Check lock owner, return false if the locked has been granted by other node.
        let get_resp = self
            .etcd_cli
            .clone()
            .get(self.shard_lock_path.clone(), None)
            .await
            .box_err()
            .context(Internal {
                msg: String::from("get shard lock failed"),
            })?;

        if !get_resp.kvs().is_empty() {
            let value = get_resp.kvs().get(0).unwrap().value();
            info!("lease exists, {:?}", get_resp);
            let str = String::from_utf8(value.to_vec())
                .box_err()
                .context(Internal {
                    msg: String::from("get response value failed"),
                })?;
            return Ok(Some(str == self.endpoint));
        }

        Ok(None)
    }

    async fn revoke_lock(&mut self) -> Result<()> {
        // 1. Stop lease.
        if let Some(sender) = self.tx.take() {
            let send_result = sender.send(());
            info!("send_result, {:?}", send_result)
        }

        // 2. Remove etcd key if it current node is shard leader
        self.etcd_cli
            .clone()
            .txn(
                Txn::new()
                    .when(vec![Compare::value(
                        self.shard_lock_path.clone(),
                        CompareOp::Equal,
                        self.endpoint.clone(),
                    )])
                    .and_then(vec![TxnOp::delete(self.shard_lock_path.clone(), None)]),
            )
            .await
            .box_err()
            .context(Internal {
                msg: String::from("etcd client txn failed"),
            })?;

        Ok(())
    }
}

impl ShardLockManager {
    pub fn new(
        cluster_name: String,
        endpoint: String,
        port: u16,
        etcd_client: Client,
    ) -> ShardLockManager {
        let addr = vec![endpoint, port.to_string()].join(":");
        ShardLockManager {
            cluster_name,
            endpoint: addr,
            root_path: "/rootPath".to_string(),
            shard_leader_key: SHARD_LEADER_PATH.to_string(),
            lease_ttl_sec: DEFAULT_LEASE_SECS,
            etcd_cli: etcd_client,
            shard_lock_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return true if grant lock success(including the current node is the
    /// owner), otherwise return false. A new thread is started internally
    /// and keeps calling keep alive.
    pub async fn grant_lock<C>(&self, shard_id: u32, lock_deleted_callback: C) -> Result<bool>
    where
        C: Fn(u32) -> Result<bool> + Send + Sync + 'static,
    {
        let mut shard_lock = ShardLock {
            shard_id,
            lock_ttl_sec: self.lease_ttl_sec,
            shard_lock_path: self.generate_key_path(shard_id),
            lock_deleted_callback: Arc::new(lock_deleted_callback),
            etcd_cli: self.etcd_cli.clone(),
            endpoint: self.endpoint.clone(),
            tx: None,
        };

        let grant_result = shard_lock.grant_lock().await;

        info!("shard_id {},grant result, {:?}", shard_id, grant_result);

        match grant_result {
            Ok(result) => {
                if result {
                    info!("grant lock,insert into map");
                    self.shard_lock_map
                        .write()
                        .await
                        .entry(shard_id)
                        .or_insert(RwLock::new(shard_lock));
                }
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    /// revoke lock is used to force transfer leader, the old leader must give
    /// up its own lock before the new leader is used.
    pub async fn revoke_lock(&self, shard_id: u32) -> Result<()> {
        let map = self.shard_lock_map.read().await;
        let shard_lock = map.get(&shard_id);
        match shard_lock {
            None => Ok(()),
            Some(lock) => lock.write().await.revoke_lock().await,
        }
    }

    fn generate_key_path(&self, shard_id: u32) -> String {
        vec![
            self.root_path.clone(),
            self.shard_leader_key.clone(),
            self.cluster_name.clone(),
            shard_id.to_string(),
        ]
        .join("/")
    }
}
