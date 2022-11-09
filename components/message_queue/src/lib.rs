// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Message queue component

pub mod kafka;
#[cfg(any(test, feature = "test"))]
pub mod tests;

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    result::Result,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub type Offset = i64;

/// Message queue interface supporting the methods needed in wal module.
#[async_trait]
pub trait MessageQueue: Debug + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type ConsumeIterator: ConsumeIterator + Send;

    async fn create_topic_if_not_exist(&self, topic_name: &str) -> Result<(), Self::Error>;

    async fn fetch_offset(
        &self,
        topic_name: &str,
        offset_type: OffsetType,
    ) -> Result<Offset, Self::Error>;

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

    async fn delete_to(&self, topic_name: &str, offset: Offset) -> Result<(), Self::Error>;
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
pub trait ConsumeIterator: Debug + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn next_message(&mut self) -> Result<(MessageAndOffset, Offset), Self::Error>;
}

/// At which position shall the stream start.
#[derive(Debug, Clone, Copy)]
pub enum StartOffset {
    /// At the earliest known offset.
    ///
    /// This might be larger than 0 if some records were already deleted due to
    /// a retention policy or delete operations.
    Earliest,

    /// At the latest known offset.
    ///
    /// This is helpful if you only want to process new data.
    Latest,

    /// At a specific offset.
    ///
    /// Note that specifying an offset that is unknown will result in the error.
    At(Offset),
}

#[derive(Debug, Clone, Copy)]
pub enum OffsetType {
    EarliestOffset,
    HighWaterMark,
}

impl Display for OffsetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetType::EarliestOffset => f.write_str("earliest_offset"),
            OffsetType::HighWaterMark => f.write_str("high_watermark"),
        }
    }
}
