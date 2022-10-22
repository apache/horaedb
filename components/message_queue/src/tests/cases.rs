// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Test cases for message queue

use crate::{
    kafka::{config::Config, kafka_impl::KafkaImpl},
    tests::util::{generate_test_data, random_topic_name},
    ConsumeIterator, Message, MessageQueue,
};

#[tokio::test]
#[ignore]
async fn test_kafka() {
    let mut config = Config::default();
    config.client_config.boost_broker = Some("127.0.0.1:9011".to_string());
    let kafka_impl = KafkaImpl::new(config).await.unwrap();

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

async fn run_message_queue_test<T: MessageQueue>(message_queue: T) {
    test_create_topic(&message_queue).await;

    test_simple_produce_consume(&message_queue).await;

    test_delete(&message_queue).await;

    test_consume_empty_topic(&message_queue).await;
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
    assert!(message_queue.delete_up_to(&topic_name, 3).await.is_ok());
    consume_all_and_compare(message_queue, &topic_name, 3, &test_messages).await;
}

async fn consume_all_and_compare<T: MessageQueue>(
    message_queue: &T,
    topic_name: &str,
    start_offset: i64,
    test_messages: &[Message],
) {
    let iter = message_queue.consume_all(topic_name).await;
    assert!(iter.is_ok());
    let mut iter = iter.unwrap();
    let mut offset = start_offset;
    let mut cnt = 0;
    while let Some(message_and_offset) = iter.next_message().await {
        assert!(message_and_offset.is_ok());
        let message_and_offset = message_and_offset.unwrap();
        assert_eq!(message_and_offset.offset, offset);
        assert_eq!(message_and_offset.message, test_messages[offset as usize]);

        offset += 1;
        cnt += 1;
    }
    assert_eq!(cnt, test_messages.len() as i64 - start_offset);
}

async fn test_consume_empty_topic<T: MessageQueue>(message_queue: &T) {
    let topic_name = random_topic_name();
    assert!(message_queue
        .create_topic_if_not_exist(topic_name.as_str())
        .await
        .is_ok());

    // Call produce to push messages at first, then call consume to pull back and
    // compare.
    let iter = message_queue.consume_all(&topic_name).await;
    assert!(iter.is_ok());
    let mut iter = iter.unwrap();
    assert!(iter.next_message().await.is_none());
}
