use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use common_types::bytes::{BytesMut, MemBufMut};
use tokio::sync::{mpsc::channel, Mutex};

use crate::{
    log_batch::PayloadDecoder,
    manager::BatchLogIteratorAdapter,
    shard_based_impl::{
        config::Config,
        read_buffer::{fetcher::Fetcher, splitter::Splitter},
    },
    tests::util::{TestIterator, TestPayloadDecoder},
};

#[tokio::test]
async fn test_basic_protocol_of_splitter_and_fetcher() {
    let test_data: Vec<_> = vec![1_u32, 2, 3, 4, 5, 6];
    let mut bytes_mut = BytesMut::with_capacity(4);
    let test_iterator = TestIterator {
        test_logs: test_data
            .iter()
            .map(|u| {
                let bytes_mut = &mut bytes_mut;
                bytes_mut.clear();
                bytes_mut.write_u32(*u).unwrap();
                bytes_mut.to_vec()
            })
            .collect(),
        cursor: 0,
        terminate: test_data.len(),
    };
    let iter_adapter = BatchLogIteratorAdapter::new_with_async(Box::new(test_iterator), 3);

    // Mock a sender of an one table shard, and its corresponding receiver.
    // Create splitter and fetcher based on them to test the basic protocol.
    let config = Config::default();
    let test_shard = 0;
    let test_table = 0;
    let (tx, rx) = channel(config.channel_size);

    let senders: BTreeMap<_, _> = [(test_table, Arc::new(tx))].into_iter().collect();
    let mut splitter = Splitter::new(test_shard, iter_adapter, senders, config.clone().into());

    let receiver = Arc::new(Mutex::new(rx));
    let mut fetcher = Fetcher::new(test_shard, test_table, receiver, config.clone().into());
    let buffer = VecDeque::with_capacity(config.fetcher_config.fetch_batch_size);
    let decoder = TestPayloadDecoder;

    let run_split = tokio::spawn(async move {
        splitter.start().await.unwrap();
    });

    let run_fetch = tokio::spawn(async move {
        let mut buffer = buffer;
        let mut res = Vec::new();
        loop {
            buffer = fetcher.fetch(buffer).await.unwrap();
            if buffer.is_empty() {
                break;
            }

            while !buffer.is_empty() {
                res.push(buffer.pop_front().unwrap());
            }
        }
        res
    });

    run_split.await.unwrap();
    let res: Vec<_> = run_fetch
        .await
        .unwrap()
        .into_iter()
        .map(|v| {
            let mut slice = v.as_slice();
            decoder.decode(&mut slice).unwrap().val
        })
        .collect();

    assert_eq!(res, test_data);
}
