use std::{cmp::Ordering, fmt::Display, sync::Arc};

use async_trait::async_trait;
use common_util::define_result;
use dashmap::DashMap;
use futures::StreamExt;
use log::info;
use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
        controller::ControllerClient,
        error::{Error as RskafkaError, ProtocolError},
        partition::{Compression, OffsetAt, PartitionClient},
        Client, ClientBuilder,
    },
    record::{Record, RecordAndOffset},
};
use snafu::{ensure, ResultExt, Snafu};

use super::config::WalConfig;
use crate::{kafka::config::Config, ConsumeAllIterator, Message, MessageAndOffset, MessageQueue};

/// The topic(with just one partition) client for Kafka
//
/// `Arc` is needed to ensure its lifetime. Because in future's gc process,
/// it may has removed from pool but still is still being used.
type TopicClientRef = Arc<PartitionClient>;
const PARTITIONS_NUM: i32 = 1;

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
        "Race happened in scanning partition in topic:{}, when:{}",
        topic_name,
        when
    ))]
    ConsumeAllRace {
        topic_name: String,
        when: ConsumeAllWhen,
    },

    #[snafu(display("Timeout happened while polling the stream for consuming all data in topic:{}, timeout_opt:{}", topic_name, timeout_opt))]
    ConsumeAllTimeout {
        topic_name: String,
        timeout_opt: String,
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

    #[snafu(display("Unknown error occurred, msg:{}", msg))]
    Unknown { msg: String },
}

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

define_result!(Error);

struct KafkaImpl {
    config: Config,
    client: Client,
    controller_client: ControllerClient,
    // TODO: maybe gc is needed for `partition_client_pool`.
    topic_client_pool: DashMap<String, TopicClientRef>,
}

impl KafkaImpl {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Kafka init, config:{:?}", config);

        if config.client_config.boost_broker.is_none() {
            panic!("The boost_broker must be set");
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
            topic_client_pool: DashMap::default(),
        })
    }

    fn get_or_create_topic_client(
        &self,
        topic_name: &str,
    ) -> std::result::Result<TopicClientRef, RskafkaError> {
        Ok(self
            .topic_client_pool
            .entry(topic_name.to_string())
            .or_insert(Arc::new(
                self.client.partition_client(topic_name, PARTITIONS_NUM)?,
            ))
            .clone())
    }
}

#[async_trait]
impl MessageQueue for KafkaImpl {
    type ConsumeAllIterator = KafkaConsumeAllIterator;
    type Error = Error;

    async fn create_topic_if_not_exist(&self, topic_name: &str) -> Result<()> {
        // Check in partition_client_pool first, maybe has exist.
        if self.topic_client_pool.contains_key(topic_name) {
            info!(
                "Topic:{} has exist in kafka and connection to the topic is still alive.",
                topic_name
            );
            return Ok(());
        }

        // Create topic in Kafka.
        let topic_creation_config = &self.config.topic_creation_config;
        info!("Try to create topic:{} in kafka", topic_name);
        let result = self
            .controller_client
            .create_topic(
                topic_name,
                PARTITIONS_NUM,
                topic_creation_config.replication_factor,
                topic_creation_config.timeout_ms,
            )
            .await;

        match result {
            // Race condition between check and creation action, that's OK.
            Ok(_) | Err(RskafkaError::ServerError(ProtocolError::TopicAlreadyExists, ..)) => Ok(()),

            Err(e) => Err(e).context(CreateTopic {
                topic_name: topic_name.to_string(),
            }),
        }
    }

    async fn produce(&self, topic_name: &str, message: Vec<Message>) -> Result<Vec<i64>> {
        let topic_client = self
            .get_or_create_topic_client(topic_name)
            .context(Produce {
                topic_name: topic_name.to_string(),
            })?;

        let records: Vec<Record> = message.into_iter().map(|m| m.into()).collect();
        Ok(topic_client
            .produce(records, Compression::default())
            .await
            .context(Produce {
                topic_name: topic_name.to_string(),
            })?)
    }

    async fn consume_all(&self, topic_name: &str) -> Result<KafkaConsumeAllIterator> {
        let topic_client = self
            .get_or_create_topic_client(topic_name)
            .context(ConsumeAll {
                topic_name: topic_name.to_string(),
                when: ConsumeAllWhen::Start,
            })?;
        KafkaConsumeAllIterator::new(topic_name, self.config.wal_config.clone(), topic_client).await
    }

    async fn delete_up_to(&self, topic_name: &str, offset: i64) -> Result<()> {
        let topic_client = self
            .get_or_create_topic_client(topic_name)
            .context(DeleteUpTo {
                topic_name: topic_name.to_string(),
                offset,
            })?;
        topic_client
            .delete_records(offset, self.config.wal_config.reader_max_wait_ms.unwrap())
            .await
            .context(DeleteUpTo {
                topic_name: topic_name.to_string(),
                offset,
            })?;
        Ok(())
    }

    // TODO: should design a stream consume method for slave node to fetch wals.
}

struct KafkaConsumeAllIterator {
    topic_name: String,
    consuming_stream: Option<StreamConsumer>,
    high_watermark: i64,
}

impl KafkaConsumeAllIterator {
    pub async fn new(
        topic_name: &str,
        config: WalConfig,
        topic_client: TopicClientRef,
    ) -> Result<Self> {
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
                topic_name: topic_name.to_string(),
                when: ConsumeAllWhen::InitIterator
            }
        );

        let mut stream_builder = StreamConsumerBuilder::new(topic_client, StartOffset::Earliest);
        let consuming_stream = if start_offset < high_watermark {
            // If not empty, make consuming stream.
            if let Some(reader_max_wait_ms) = config.reader_max_wait_ms {
                stream_builder = stream_builder.with_max_wait_ms(reader_max_wait_ms)
            }

            if let Some(reader_min_batch_size) = config.reader_min_batch_size {
                stream_builder = stream_builder.with_min_batch_size(reader_min_batch_size)
            }

            if let Some(reader_max_batch_size) = config.reader_max_batch_size {
                stream_builder = stream_builder.with_min_batch_size(reader_max_batch_size)
            }

            Some(stream_builder.build())
        } else {
            None
        };

        Ok(KafkaConsumeAllIterator {
            topic_name: topic_name.to_string(),
            consuming_stream,
            high_watermark,
        })
    }
}

#[async_trait]
impl ConsumeAllIterator for KafkaConsumeAllIterator {
    type Error = Error;

    async fn next_message(&mut self) -> Option<Result<MessageAndOffset>> {
        let stream = match &mut self.consuming_stream {
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
                        when: ConsumeAllWhen::PollStream,
                    }
                    .fail(),
                ),

                Ordering::Less => Some(
                    Unknown {
                        msg: format!(
                            "High watermark decrease while consuming all data in topic:{}",
                            self.topic_name
                        ),
                    }
                    .fail(),
                ),

                Ordering::Equal => {
                    if record.offset + 1 == self.high_watermark {
                        info!("Consume all data successfully in topic:{}", self.topic_name);
                        self.consuming_stream = None;
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
                        "Consuming stream return None due to unknown cause, topic:{}",
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
