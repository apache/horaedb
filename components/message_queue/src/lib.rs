// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Message queue component

pub mod kafka;
#[cfg(any(test, feature = "test"))]
pub mod tests;

use std::{collections::BTreeMap, result::Result};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub type Offset = i64;

/// Message queue interface supporting the methods needed in wal module.
#[async_trait]
pub trait MessageQueue: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type ConsumeIterator: ConsumeIterator + Send;

    async fn create_topic_if_not_exist(&self, topic_name: &str) -> Result<(), Self::Error>;

    async fn get_earliest_offset(&self, topic_name: &str) -> Result<Offset, Self::Error>;

    async fn get_high_watermark(&self, topic_name: &str) -> Result<Offset, Self::Error>;

    async fn produce(
        &self,
        topic_name: &str,
        messages: Vec<Message>,
    ) -> Result<Vec<Offset>, Self::Error>;

    async fn consume(
        &self,
        topic_name: &str,
        start_offset: StartOffset,
    ) -> Result<Self::ConsumeIterator, Self::Error>;

    async fn delete_up_to(&self, topic_name: &str, offset: Offset) -> Result<(), Self::Error>;
    // TODO: should design a stream consume method for slave node to fetch wals.
}

/// High-level record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: DateTime<Utc>,
}

/// Record that has offset information attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAndOffset {
    pub message: Message,
    pub offset: Offset,
}

#[async_trait]
pub trait ConsumeIterator {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn next_message(&mut self) -> Result<(MessageAndOffset, Offset), Self::Error>;
}

/// At which position shall the stream start.
#[derive(Debug, Clone, Copy)]
pub enum StartOffset {
    /// At the earlist known offset.
    ///
    /// This might be larger than 0 if some records were already deleted due to
    /// a retention policy or delete records.
    Earliest,

    /// At the latest known offset.
    ///
    /// This is helpful if you only want ot process new data.
    Latest,

    /// At a specific offset.
    ///
    /// NOTICE: if the setting start offset smaller than the earliest
    /// offset in topic('s partition), it will be reset to the earliest
    /// offset automatically.
    At(Offset),
}
