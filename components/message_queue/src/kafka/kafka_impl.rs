// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Kafka implementation's detail

use std::{cmp::Ordering, collections::HashMap, fmt::Display, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use futures::StreamExt;
use log::info;
use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
        controller::ControllerClient,
        error::{Error as RskafkaError, ProtocolError},
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
        Client, ClientBuilder,
    },
    record::{Record, RecordAndOffset},
};
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    kafka::config::{Config, ConsumerConfig},
    ConsumeIterator, Message, MessageAndOffset, MessageQueue, Offset,
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

    #[snafu(display("Failed to produce to kafka topic:{}, err:{}", topic_name, source))]
    Produce {
        topic_name: String,
        source: RskafkaError,
    },

    #[snafu(display(
        "Failed to consume all data in topic:{} when:{}, source:{}",
        topic_name,
        when,
        source
    ))]
    ConsumeAll {
        topic_name: String,
        source: RskafkaError,
        when: ConsumeAllWhen,
    },

    #[snafu(display(
        "Race happened in scanning partition in topic:{}, when:{}, msg:[{}], backtrace:{}",
        topic_name,
        when,
        msg,
        backtrace
    ))]
    ConsumeAllRace {
        topic_name: String,
        when: ConsumeAllWhen,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Timeout happened while polling the stream for consuming all data in topic:{}, timeout_opt:{}, backtrace:{}", topic_name, timeout_opt, backtrace))]
    ConsumeAllTimeout {
        topic_name: String,
        timeout_opt: String,
        backtrace: Backtrace,
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
pub enum ConsumeAllWhen {
    Start,
    InitIterator,
    PollStream,
}

impl Display for ConsumeAllWhen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumeAllWhen::Start => f.write_str("start"),
            ConsumeAllWhen::InitIterator => f.write_str("init_iterator"),
            ConsumeAllWhen::PollStream => f.write_str("poll_stream"),
        }
    }
}

pub struct KafkaImpl {
    config: Config,
    client: Client,
    controller_client: ControllerClient,
    // TODO: maybe gc is needed for `partition_client_pool`.
    topic_client_pool: RwLock<HashMap<String, TopicClientRef>>,
}

impl KafkaImpl {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Kafka init, config:{:?}", config);

        if config.client_config.boost_broker.is_none() {
            panic!("The boost broker must be set");
        }

        let mut client_builder =
            ClientBuilder::new(vec![config.client_config.boost_broker.clone().unwrap()]);
        if let Some(max_message_size) = config.client_config.max_message_size {
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
            let topic_client_pool = self.topic_client_pool.read().await;

            if topic_client_pool.contains_key(topic_name) {
                info!(
                    "Topic:{} has exist in kafka and connection to the topic is still alive",
                    topic_name
                );
                return Ok(());
            }
        }

        // Create topic in Kafka.
        let topic_management_config = &self.config.topic_management_config;
        info!("Try to create topic:{} in kafka", topic_name);
        let result = self
            .controller_client
            .create_topic(
                topic_name,
                PARTITION_NUM,
                topic_management_config.create_replication_factor,
                topic_management_config.create_max_wait_ms,
            )
            .await;

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

    async fn consume_all(&self, topic_name: &str) -> Result<KafkaConsumeIterator> {
        info!("Need to consume all data in kafka topic:{}", topic_name);

        let topic_client =
            self.get_or_create_topic_client(topic_name)
                .await
                .context(ConsumeAll {
                    topic_name: topic_name.to_string(),
                    when: ConsumeAllWhen::Start,
                })?;
        KafkaConsumeIterator::new(
            topic_name,
            self.config.consumer_config.clone(),
            topic_client,
        )
        .await
    }

    async fn delete_up_to(&self, topic_name: &str, offset: Offset) -> Result<()> {
        let topic_client =
            self.get_or_create_topic_client(topic_name)
                .await
                .context(DeleteUpTo {
                    topic_name: topic_name.to_string(),
                    offset,
                })?;

        topic_client
            .delete_records(
                offset,
                self.config.topic_management_config.delete_max_wait_ms,
            )
            .await
            .context(DeleteUpTo {
                topic_name: topic_name.to_string(),
                offset,
            })?;

        Ok(())
    }

    // TODO: should design a stream consume method for slave node to fetch wals.
}

pub struct KafkaConsumeIterator {
    topic_name: String,
    stream_consumer: Option<StreamConsumer>,
    high_watermark: i64,
}

impl KafkaConsumeIterator {
    pub async fn new(
        topic_name: &str,
        config: ConsumerConfig,
        topic_client: TopicClientRef,
    ) -> Result<Self> {
        info!("Init consumer of topic:{}, config:{:?}", topic_name, config);

        // We should make sure the partition is not empty firstly.
        let start_offset =
            topic_client
                .get_offset(OffsetAt::Earliest)
                .await
                .context(ConsumeAll {
                    topic_name: topic_name.to_string(),
                    when: ConsumeAllWhen::InitIterator,
                })?;
        let high_watermark =
            topic_client
                .get_offset(OffsetAt::Latest)
                .await
                .context(ConsumeAll {
                    topic_name: topic_name.to_string(),
                    when: ConsumeAllWhen::InitIterator,
                })?;
        ensure!(
            start_offset <= high_watermark,
            ConsumeAllRace {
                topic_name,
                msg: format!(
                    "high watermark:{} is smaller than start offset:{}",
                    high_watermark, start_offset
                ),
                when: ConsumeAllWhen::InitIterator
            }
        );

        let mut stream_builder = StreamConsumerBuilder::new(topic_client, StartOffset::Earliest);
        let stream_consumer = if start_offset < high_watermark {
            // If not empty, make consuming stream.
            if let Some(max_wait_ms) = config.max_wait_ms {
                stream_builder = stream_builder.with_max_wait_ms(max_wait_ms)
            }

            if let Some(min_batch_size) = config.min_batch_size {
                stream_builder = stream_builder.with_min_batch_size(min_batch_size)
            }

            if let Some(max_batch_size) = config.max_batch_size {
                stream_builder = stream_builder.with_min_batch_size(max_batch_size)
            }

            Some(stream_builder.build())
        } else {
            None
        };

        Ok(KafkaConsumeIterator {
            topic_name: topic_name.to_string(),
            stream_consumer,
            high_watermark,
        })
    }
}

#[async_trait]
impl ConsumeIterator for KafkaConsumeIterator {
    type Error = Error;

    async fn next_message(&mut self) -> Option<Result<MessageAndOffset>> {
        let stream = match &mut self.stream_consumer {
            Some(stream) => stream,
            None => {
                return None;
            }
        };

        // Return message and offset from buffer.
        match stream.next().await {
            Some(Ok((record, high_watermark))) => match high_watermark.cmp(&self.high_watermark) {
                Ordering::Greater => Some(
                    ConsumeAllRace {
                        topic_name: self.topic_name.clone(),
                        msg: format!("remote high watermark:{} is greater than local:{}", high_watermark, self.high_watermark),
                        when: ConsumeAllWhen::PollStream,
                    }
                    .fail(),
                ),

                Ordering::Less => Some(
                    Unknown {
                        msg: format!(
                            "remote high watermark:{} is less than local:{} in topic:{}, it shouldn't decrease while consuming all data",
                            high_watermark,
                            self.high_watermark,
                            self.topic_name
                        ),
                    }
                    .fail(),
                ),

                Ordering::Equal => {
                    if record.offset + 1 == self.high_watermark {
                        info!("Consume all data successfully in topic:{}", self.topic_name);
                        self.stream_consumer = None;
                    }

                    Some(Ok(record.into()))
                }
            },

            Some(Err(e)) => Some(Err(e).context(ConsumeAll {
                topic_name: self.topic_name.clone(),
                when: ConsumeAllWhen::PollStream,
            })),

            None => Some(
                Unknown {
                    msg: format!(
                        "consuming stream return None due to unknown cause, topic:{}",
                        self.topic_name
                    ),
                }
                .fail(),
            ),
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
