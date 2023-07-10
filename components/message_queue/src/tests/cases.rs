// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Test cases for message queue

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::time::timeout;

use crate::{
    kafka::{config::Config, kafka_impl::KafkaImpl},
    tests::util::{generate_test_data, random_topic_name},
    ConsumeIterator, Message, MessageQueue, OffsetType, StartOffset,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "It can just run with a Kafka cluster"]
async fn test_kafka() {
    let mut config = Config::default();
    config.client.boost_brokers = Some(vec!["127.0.0.1:9011".to_string()]);
    let kafka_impl = Arc::new(KafkaImpl::new(config).await.unwrap());

    run_message_queue_test(kafka_impl).await;
}

async fn test_create_topic<T: MessageQueue>(message_queue: &T) {
    assert!(message_queue
        .create_topic_if_not_exist(random_topic_name().as_str())
        .await
        .is_ok());
    // Topic has already existed is ok.
    assert!(message_queue
        .create_topic_if_not_exist(random_topic_name().as_str())
        .await
        .is_ok());
}

async fn run_message_queue_test<T: MessageQueue>(message_queue: Arc<T>) {
    test_create_topic(message_queue.as_ref()).await;

    test_simple_produce_consume(message_queue.as_ref()).await;

    test_delete(message_queue.as_ref()).await;

    test_consume_empty_topic(message_queue.as_ref()).await;

    test_consume_fetch_offset(message_queue.as_ref()).await;

    test_multiple_consumer_on_same_topic(message_queue.clone()).await;
}

async fn test_simple_produce_consume<T: MessageQueue>(message_queue: &T) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // Call produce to push messages at first, then call consume to pull back and
    // compare.
    let test_messages = generate_test_data(10);
    assert!(message_queue
        .produce(&topic_name, test_messages.clone())
        .await
        .is_ok());
    consume_all_and_compare(message_queue, &topic_name, 0, &test_messages).await;
}

async fn test_delete<T: MessageQueue>(message_queue: &T) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // Test consume and produce.
    let test_messages = generate_test_data(10);
    assert!(message_queue
        .produce(&topic_name, test_messages.clone())
        .await
        .is_ok());
    consume_all_and_compare(message_queue, &topic_name, 0, &test_messages).await;

    // Test consume after deleting.
    assert!(message_queue.delete_to(&topic_name, 3).await.is_ok());
    consume_all_and_compare(message_queue, &topic_name, 3, &test_messages).await;
}

async fn consume_all_and_compare<T: MessageQueue>(
    message_queue: &T,
    topic_name: &str,
    start_offset: i64,
    test_messages: &[Message],
) {
    let iter = message_queue
        .consume(topic_name, StartOffset::Earliest)
        .await;
    let high_watermark = message_queue
        .fetch_offset(topic_name, OffsetType::HighWaterMark)
        .await
        .unwrap();
    assert!(iter.is_ok());
    let mut iter = iter.unwrap();
    let mut offset = start_offset;
    let mut cnt = 0;

    loop {
        let res = iter.next_message().await;
        assert!(res.is_ok());
        let (message_and_offset, _) = res.unwrap();
        assert_eq!(message_and_offset.offset, offset);
        assert_eq!(message_and_offset.message, test_messages[offset as usize]);

        offset += 1;
        cnt += 1;

        if message_and_offset.offset + 1 == high_watermark {
            break;
        }
    }
    assert_eq!(cnt, test_messages.len() as i64 - start_offset);
}

async fn test_consume_empty_topic<T: MessageQueue>(message_queue: &T) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // FIXME: consume a empty topic may be hanged forever...
    let mut iter = message_queue
        .consume(&topic_name, StartOffset::Earliest)
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(1000), iter.next_message())
        .await
        .is_err());
}

async fn test_consume_fetch_offset<T: MessageQueue>(message_queue: &T) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // At the beginning, the topic's partition is empty, earliest offset and high
    // watermark should be zero.
    let earliest_offset = message_queue
        .fetch_offset(&topic_name, OffsetType::EarliestOffset)
        .await
        .unwrap();
    let high_watermark = message_queue
        .fetch_offset(&topic_name, OffsetType::HighWaterMark)
        .await
        .unwrap();
    assert_eq!(earliest_offset, 0);
    assert_eq!(high_watermark, 0);

    // We produce so messages into it, earliest is still zero, but high watermark
    // will equal to the amount of messages.
    let test_messages = generate_test_data(10);
    assert!(message_queue
        .produce(&topic_name, test_messages.clone())
        .await
        .is_ok());
    let earliest_offset = message_queue
        .fetch_offset(&topic_name, OffsetType::EarliestOffset)
        .await
        .unwrap();
    let high_watermark = message_queue
        .fetch_offset(&topic_name, OffsetType::HighWaterMark)
        .await
        .unwrap();
    assert_eq!(earliest_offset, 0);
    assert_eq!(high_watermark, 10);

    // We delete some messages later, and the earliest offset will become the offset
    // which is deleted to.
    assert!(message_queue.delete_to(&topic_name, 3).await.is_ok());
    let earliest_offset = message_queue
        .fetch_offset(&topic_name, OffsetType::EarliestOffset)
        .await
        .unwrap();
    let high_watermark = message_queue
        .fetch_offset(&topic_name, OffsetType::HighWaterMark)
        .await
        .unwrap();
    assert_eq!(earliest_offset, 3);
    assert_eq!(high_watermark, 10);
}

async fn test_multiple_consumer_on_same_topic<T: MessageQueue>(message_queue: Arc<T>) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // Call produce to push messages at first, then call consume in two tasks to
    // pull back and compare.
    let test_messages = generate_test_data(10);
    assert!(message_queue
        .produce(&topic_name, test_messages.clone())
        .await
        .is_ok());

    let is_start = Arc::new(AtomicBool::new(false));

    let message_queue_clone = message_queue.clone();
    let topic_name_clone = topic_name.clone();
    let test_messages_clone = test_messages.clone();
    let is_start_clone = is_start.clone();
    let handle1 = tokio::spawn(async move {
        while !is_start_clone.load(Ordering::SeqCst) {}

        consume_all_and_compare(
            message_queue_clone.as_ref(),
            &topic_name_clone,
            0,
            &test_messages_clone,
        )
        .await;
    });

    let is_start_clone = is_start.clone();
    let handle2 = tokio::spawn(async move {
        while !is_start_clone.load(Ordering::SeqCst) {}

        consume_all_and_compare(message_queue.as_ref(), &topic_name, 0, &test_messages).await;
    });

    // Let them start and join the handles.
    is_start.store(true, Ordering::SeqCst);
    let _ = handle1.await;
    let _ = handle2.await;
}
