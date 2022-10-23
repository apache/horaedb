// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal's read buffer, used by shard based wal manager while reading

mod error;
pub mod fetcher;
pub mod splitter;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use common_types::table::{ShardId, TableId};
use error::*;
use log::info;
use snafu::ensure;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, RwLock,
};

use crate::manager::WalManagerRef;

/// Wal's read buffer, implemented by channel which are organized like this:
///                                                    +---------------+
///                                              +-----| Table channel |
///                                              |     +---------------+
///                         +----------------+   |                      
///                    +----|Shard's channels|---+           ...        
///                    |    +----------------+   |                      
/// +--------------+   |                         |     +---------------+
/// |              |---+          ...            +-----| Table channel |
/// | Read buffer  |---+                               +---------------+
/// |              |   |    +----------------+                       
/// +--------------+   +----|Shard's channels|                          
///                         +----------------+    
struct ReadBuffer {
    shard_channels: ShardChannelsRef,
}

impl ReadBuffer {
    async fn insert_shard_channels(&self, shard_id: ShardId, channels: TableChannels) -> bool {
        let mut shard_channels = self.shard_channels.write().await;
        if let std::collections::hash_map::Entry::Vacant(e) = shard_channels.entry(shard_id) {
            e.insert(channels);
            true
        } else {
            // Shard has exist is abnormal, error occurred on high-level.
            false
        }
    }

    async fn remove_shard_channels(&self, shard_id: ShardId) -> bool {
        let mut shard_channels = self.shard_channels.write().await;
        if shard_channels.contains_key(&shard_id) {
            shard_channels.remove(&shard_id);
            true
        } else {
            // Shard not exist is abnormal, error occurred on high-level.
            false
        }
    }

    #[allow(dead_code)]
    async fn get_shard_channels(
        &self,
        shard_id: ShardId,
    ) -> Option<BTreeMap<TableId, TableChannel>> {
        let shard_channels = self.shard_channels.read().await;
        shard_channels
            .get(&shard_id)
            .map(|channels| channels.iter().map(|(k, v)| (*k, v.clone())).collect())
    }

    // TODO: may be better to tell shard not found or table not found on shard?
    #[allow(dead_code)]
    async fn get_table_channel(
        &self,
        shard_id: ShardId,
        table_id: TableId,
    ) -> Option<TableChannel> {
        let shard_channels = self.shard_channels.read().await;
        shard_channels.get(&shard_id)?.get(&table_id).cloned()
    }
}

#[derive(Debug)]
pub enum LogMessage {
    Content(Vec<u8>),
    End,
}

type LogSenderRef = Arc<Sender<LogMessage>>;
// No race actually, we want to control its lifetime by `Arc`,
// but `&mut` is needed for receiver to call `recv`, so `Arc<Mutex<...>>`
// is necessary.
type LogReceiverRef = Arc<Mutex<Receiver<LogMessage>>>;

#[derive(Clone)]
struct TableChannel {
    sender: LogSenderRef,
    receiver: LogReceiverRef,
}

type TableChannels = HashMap<TableId, TableChannel>;
type ShardChannels = HashMap<ShardId, TableChannels>;
type ShardChannelsRef = Arc<RwLock<ShardChannels>>;

/// [ReadBuffer]'s manager
pub struct ReadBufferManager {
    buffer: ReadBuffer,
    channel_size: usize,
    wal_manager: WalManagerRef,
}

impl ReadBufferManager {
    pub async fn register_shard(&self, sheet: RegistrationSheet) -> Result<()> {
        info!(
            "Register shard to wal read buffer, registration sheet:{:?}",
            sheet
        );

        let table_channels: HashMap<_, _> = sheet
            .table_ids
            .iter()
            .map(|id| {
                let (tx, rx) = channel(self.channel_size);
                (
                    *id,
                    TableChannel {
                        sender: Arc::new(tx),
                        receiver: Arc::new(Mutex::new(rx)),
                    },
                )
            })
            .collect();

        ensure!(
            self.buffer
                .insert_shard_channels(sheet.shard_id, table_channels)
                .await,
            RegisterShard {
                sheet: sheet.clone()
            }
        );
        Ok(())
    }

    pub async fn unregister_shard(&self, shard_id: ShardId) -> Result<()> {
        info!("Unregister shard from wal read buffer, shard:{}", shard_id);

        ensure!(
            self.buffer.remove_shard_channels(shard_id).await,
            UnregisterShard { shard_id }
        );
        Ok(())
    }

    // pub async fn get_shard_splitter(&self, shard_id: ShardId) -> Result<>
}

#[derive(Debug, Clone)]
pub struct RegistrationSheet {
    pub shard_id: ShardId,
    pub table_ids: Vec<TableId>,
}

impl RegistrationSheet {
    pub fn new(shard_id: ShardId, table_ids: Vec<TableId>) -> Self {
        Self {
            shard_id,
            table_ids,
        }
    }
}
