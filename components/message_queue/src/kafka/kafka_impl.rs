// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Kafka implementation's detail

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use common_util::define_result;
use futures::StreamExt;
use log::info;
use rskafka::{
    client::{
        consumer::{StartOffset as KafkaStartOffset, StreamConsumer, StreamConsumerBuilder},
        controller::ControllerClient,
        error::{Error as RskafkaError, ProtocolError},
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
        Client, ClientBuilder,
    },
    record::{Record, RecordAndOffset},
};
use snafu::{Backtrace, ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    kafka::config::{Config, ConsumerConfig},
    ConsumeIterator, Message, MessageAndOffset, MessageQueue, Offset, OffsetType, StartOffset,
};

/// The topic (with just one partition) client for Kafka
//
/// `Arc` is needed to ensure its lifetime because in future's gc process,
/// it may has removed from pool but be still in use.
type TopicClientRef = Arc<PartitionClient>;
const PARTITION_NUM: i32 = 1;
const DEFAULT_PARTITION: i32 = 0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to init kafka client, err:{}", source))]
    Init { source: RskafkaError },

    #[snafu(display("Failed to list topics in kafka, err:{}", source))]
    ListTopics { source: RskafkaError },

    #[snafu(display("Failed to create topic in kafka:{}, err:{}", topic_name, source))]
    CreateTopic {
        topic_name: String,
        source: RskafkaError,
    },

    #[snafu(display(
        "Failed to fetch offset(type:{}) from kafka topic:{}, err:{}",
        offset_type,
        topic_name,
        source
    ))]
    FetchOffset {
        topic_name: String,
        source: RskafkaError,
        offset_type: OffsetType,
    },

    #[snafu(display("Failed to produce to kafka topic:{}, err:{}", topic_name, source))]
    Produce {
        topic_name: String,
        source: RskafkaError,
    },

    #[snafu(display(
        "Failed to consume in topic:{} when:{}, source:{}",
        topic_name,
        when,
        source
    ))]
    Consume {
        topic_name: String,
        source: RskafkaError,
        when: ConsumeWhen,
    },

    #[snafu(display(
        "Failed to produce to kafka topic:{}, offset:{}, err:{}",
        topic_name,
        offset,
        source
    ))]
    DeleteUpTo {
        topic_name: String,
        offset: i64,
        source: RskafkaError,
    },

    #[snafu(display("Unknown error occurred, msg:[{}], backtrace:{}", msg, backtrace))]
    Unknown { msg: String, backtrace: Backtrace },
}

define_result!(Error);

#[derive(Debug)]
pub enum ConsumeWhen {
    Start,
    PollStream,
}

impl Display for ConsumeWhen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeWhen::Start => f.write_str("start"),
            ConsumeWhen::PollStream => f.write_str("poll_stream"),
        }
    }
}

#[derive(Clone)]
pub struct KafkaImpl(Arc<KafkaImplInner>);

impl KafkaImpl {
    pub async fn new(config: Config) -> Result<Self> {
        let inner = KafkaImplInner::new(config).await?;
        Ok(Self(Arc::new(inner)))
    }
}

struct KafkaImplInner {
    config: Config,
    client: Client,
    controller_client: ControllerClient,
    // TODO: maybe gc is needed for `partition_client_pool`.
    topic_client_pool: RwLock<HashMap<String, TopicClientRef>>,
}

impl KafkaImplInner {
    async fn new(config: Config) -> Result<Self> {
        info!("Kafka init, config:{:?}", config);

        if config.client.boost_brokers.is_none() {
            panic!("The boost broker must be set");
        }

        let mut client_builder =
            ClientBuilder::new(dbg!(config.client.boost_brokers.clone().unwrap()));
        if let Some(max_message_size) = config.client.max_message_size {
            client_builder = client_builder.max_message_size(max_message_size);
        }

        let client = client_builder.build().await.context(Init)?;

        let controller_client = client.controller_client().context(Init)?;

        Ok(Self {
            config,
            client,
            controller_client,
            topic_client_pool: RwLock::new(HashMap::new()),
        })
    }

    async fn get_or_create_topic_client(
        &self,
        topic_name: &str,
    ) -> std::result::Result<TopicClientRef, RskafkaError> {
        {
            let topic_client_pool = self.topic_client_pool.read().await;
            // If found, just return it
            if let Some(client) = topic_client_pool.get(topic_name) {
                return Ok(client.clone());
            }
        }

        // Otherwise, we should make a double-check first,
        // and if still not found(other thread may has inserted it),
        // we should create it.
        let mut topic_client_pool = self.topic_client_pool.write().await;
        if let Some(client) = topic_client_pool.get(topic_name) {
            Ok(client.clone())
        } else {
            let client = Arc::new(
                self.client
                    .partition_client(topic_name, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
                    .await?,
            );
            topic_client_pool.insert(topic_name.to_string(), client.clone());
            Ok(client)
        }
    }
}

#[async_trait]
impl MessageQueue for KafkaImpl {
    type ConsumeIterator = KafkaConsumeIterator;
    type Error = Error;

    async fn create_topic_if_not_exist(&self, topic_name: &str) -> Result<()> {
        // Check in partition_client_pool first, maybe has exist.
        {
            let topic_client_pool = self.0.topic_client_pool.read().await;

            if topic_client_pool.contains_key(topic_name) {
                info!(
                    "Topic:{} has exist in kafka and connection to the topic is still alive",
                    topic_name
                );
                return Ok(());
            }
        }

        // Create topic in Kafka.
        let topic_management_config = &self.0.config.topic_management;
        info!("Try to create topic, name:{}.", topic_name);
        let result = self
            .0
            .controller_client
            .create_topic(
                topic_name,
                PARTITION_NUM,
                topic_management_config.create_replication_factor,
                topic_management_config.create_max_wait_ms,
            )
            .await;

        info!(
            "Create topic finish, name:{}, result:{:?}",
            topic_name, result
        );
        match result {
            // Race condition between check and creation action, that's OK.
            Ok(_)
            | Err(RskafkaError::ServerError {
                protocol_error: ProtocolError::TopicAlreadyExists,
                ..
            }) => Ok(()),

            Err(e) => Err(e).context(CreateTopic {
                topic_name: topic_name.to_string(),
            }),
        }
    }

    async fn produce(&self, topic_name: &str, messages: Vec<Message>) -> Result<Vec<Offset>> {
        let topic_client = self
            .0
            .get_or_create_topic_client(topic_name)
            .await
            .context(Produce {
                topic_name: topic_name.to_string(),
            })?;

        let records: Vec<Record> = messages.into_iter().map(|m| m.into()).collect();
        Ok(topic_client
            .produce(records, Compression::default())
            .await
            .context(Produce {
                topic_name: topic_name.to_string(),
            })?)
    }

    async fn fetch_offset(&self, topic_name: &str, offset_type: OffsetType) -> Result<Offset> {
        let topic_client = self
            .0
            .get_or_create_topic_client(topic_name)
            .await
            .context(FetchOffset {
                topic_name: topic_name.to_string(),
                offset_type,
            })?;

        topic_client
            .get_offset(offset_type.into())
            .await
            .context(FetchOffset {
                topic_name: topic_name.to_string(),
                offset_type,
            })
    }

    async fn consume(
        &self,
        topic_name: &str,
        start_offset: StartOffset,
    ) -> Result<KafkaConsumeIterator> {
        info!("Consume data in kafka topic:{}", topic_name);

        let topic_client = self
            .0
            .get_or_create_topic_client(topic_name)
            .await
            .context(Consume {
                topic_name: topic_name.to_string(),
                when: ConsumeWhen::Start,
            })?;
        Ok(KafkaConsumeIterator::new(
            topic_name,
            self.0.config.consumer.clone(),
            topic_client,
            start_offset,
        ))
    }

    async fn delete_to(&self, topic_name: &str, offset: Offset) -> Result<()> {
        let topic_client = self
            .0
            .get_or_create_topic_client(topic_name)
            .await
            .context(DeleteUpTo {
                topic_name: topic_name.to_string(),
                offset,
            })?;

        topic_client
            .delete_records(offset, self.0.config.topic_management.delete_max_wait_ms)
            .await
            .context(DeleteUpTo {
                topic_name: topic_name.to_string(),
                offset,
            })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct KafkaConsumeIterator {
    topic_name: String,
    stream_consumer: StreamConsumer,
}

impl KafkaConsumeIterator {
    pub fn new(
        topic_name: &str,
        config: ConsumerConfig,
        topic_client: TopicClientRef,
        start_offset: StartOffset,
    ) -> Self {
        info!("Init consumer of topic:{}, config:{:?}", topic_name, config);

        // If not empty, make consuming stream.
        let stream_consumer = {
            let mut stream_builder = StreamConsumerBuilder::new(topic_client, start_offset.into());

            if let Some(max_wait_ms) = config.max_wait_ms {
                stream_builder = stream_builder.with_max_wait_ms(max_wait_ms)
            }

            if let Some(min_batch_size) = config.min_batch_size {
                stream_builder = stream_builder.with_min_batch_size(min_batch_size)
            }

            if let Some(max_batch_size) = config.max_batch_size {
                stream_builder = stream_builder.with_min_batch_size(max_batch_size)
            }

            stream_builder.build()
        };

        KafkaConsumeIterator {
            topic_name: topic_name.to_string(),
            stream_consumer,
        }
    }
}

#[async_trait]
impl ConsumeIterator for KafkaConsumeIterator {
    type Error = Error;

    async fn next_message(&mut self) -> Result<(MessageAndOffset, Offset)> {
        // Return message and offset from buffer.
        match self.stream_consumer.next().await {
            Some(Ok((record, high_watermark))) => Ok((record.into(), high_watermark)),

            Some(Err(e)) => Err(e).context(Consume {
                topic_name: self.topic_name.clone(),
                when: ConsumeWhen::PollStream,
            }),

            None => Unknown {
                msg: format!(
                    "consuming stream return None due to unknown cause, topic:{}",
                    self.topic_name
                ),
            }
            .fail(),
        }
    }
}

impl From<Message> for Record {
    fn from(message: Message) -> Self {
        Self {
            key: message.key,
            value: message.value,
            headers: message.headers,
            timestamp: message.timestamp,
        }
    }
}

impl From<RecordAndOffset> for MessageAndOffset {
    fn from(record_and_offset: RecordAndOffset) -> Self {
        let message = Message {
            key: record_and_offset.record.key,
            value: record_and_offset.record.value,
            headers: record_and_offset.record.headers,
            timestamp: record_and_offset.record.timestamp,
        };

        Self {
            message,
            offset: record_and_offset.offset,
        }
    }
}

impl From<StartOffset> for KafkaStartOffset {
    fn from(start_offset: StartOffset) -> Self {
        match start_offset {
            StartOffset::Earliest => KafkaStartOffset::Earliest,
            StartOffset::Latest => KafkaStartOffset::Latest,
            StartOffset::At(offset) => KafkaStartOffset::At(offset),
        }
    }
}

impl From<OffsetType> for OffsetAt {
    fn from(offset_type: OffsetType) -> Self {
        match offset_type {
            OffsetType::EarliestOffset => OffsetAt::Earliest,
            OffsetType::HighWaterMark => OffsetAt::Latest,
        }
    }
}

impl Debug for KafkaImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaImpl")
            .field("config", &self.0.config)
            .field("client", &"rskafka".to_string())
            .finish()
    }
}
