use serde_derive::{Deserialize, Serialize};

/// Generic client config that is used for consumers, producers as well as admin
/// operations (like "create topic").
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub client_config: ClientConfig,
    pub topic_creation_config: TopicCreationConfig,
    pub wal_config: WalConfig,
}

/// Generic client config that is used for consumers, producers as well as admin
/// operations (like "create topic").
#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientConfig {
    /// The endpoint of boost broker, must be set and will panic if not.
    pub boost_broker: Option<String>,

    /// Maximum message size in bytes.
    ///
    /// extracted from `max_message_size`. Defaults to `None` (rskafka default).
    pub max_message_size: Option<usize>,

    /// Optional SOCKS5 proxy to use for connecting to the brokers.
    ///
    /// extracted from `socks5_proxy`. Defaults to `None`.
    pub socks5_proxy: Option<String>,
}

/// Config for topic creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicCreationConfig {
    /// Replication factor.
    ///
    /// Extracted from `replication_factor` option. Defaults to `1`.
    pub replication_factor: i16,

    /// Timeout in ms.
    ///
    /// Extracted from `timeout_ms` option. Defaults to `5_000`.
    pub timeout_ms: i32,
}

impl Default for TopicCreationConfig {
    fn default() -> Self {
        Self {
            replication_factor: 1,
            timeout_ms: 5000,
        }
    }
}

/// Config for consumers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// Will wait for at least `min_batch_size` bytes of data
    ///
    /// Extracted from `consumer_max_wait_ms`. Defaults to `None` (rskafka
    /// default).
    pub reader_max_wait_ms: Option<i32>,

    /// The maximum amount of data to fetch in a single batch
    ///
    /// Extracted from `consumer_min_batch_size`. Defaults to `None` (rskafka
    /// default).
    pub reader_min_batch_size: Option<i32>,

    /// The maximum amount of time to wait for data before returning
    ///
    /// Extracted from `consumer_max_batch_size`. Defaults to `None` (rskafka
    /// default).
    pub reader_max_batch_size: Option<i32>,

    pub reader_consume_all_wait_ms: i32,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            reader_max_wait_ms: Default::default(),
            reader_min_batch_size: Default::default(),
            reader_max_batch_size: Default::default(),
            reader_consume_all_wait_ms: 5000,
        }
    }
}
