pub mod kafka;

use std::{collections::BTreeMap, result::Result};

use async_trait::async_trait;
use time::OffsetDateTime;

/// Topic's producer
///
/// In ceresdb every topic just has one partition.
/// Because there won't be a huge number of topic(the upper bound is just about
/// 1000).
#[async_trait]
pub trait MessageQueue: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type ConsumeAllIterator: ConsumeAllIterator + Send;

    async fn create_topic_if_not_exist(&self, topic_name: &str) -> Result<(), Self::Error>;
    async fn produce(
        &self,
        topic_name: &str,
        message: Vec<Message>,
    ) -> Result<Vec<i64>, Self::Error>;
    async fn consume_all(&self, topic_name: &str) -> Result<Self::ConsumeAllIterator, Self::Error>;
    async fn delete_up_to(&self, topic_name: &str, offset: i64) -> Result<(), Self::Error>;
    // TODO: should design a stream consume method for slave node to fetch wals.
}

/// High-level record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: BTreeMap<String, Vec<u8>>,
    pub timestamp: OffsetDateTime,
}

/// Record that has offset information attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAndOffset {
    pub message: Message,
    pub offset: i64,
}

#[async_trait]
pub trait ConsumeAllIterator {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn next_message(&mut self) -> Option<Result<MessageAndOffset, Self::Error>>;
}
