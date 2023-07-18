// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// A stream can be prefetchable.

use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};

pub type BoxedStream<T> = Box<dyn Stream<Item = T> + Send + Unpin>;

#[async_trait]
pub trait PrefetchableStream: Send {
    type Item;

    /// Start the prefetch procedure in background. In most implementation, this
    /// method should not block the caller, that is to say, the prefetching
    /// procedure should be run in the background.
    async fn start_prefetch(&mut self);

    /// Fetch the next record batch.
    ///
    /// If None is returned, all the following batches will be None.
    async fn fetch_next(&mut self) -> Option<Self::Item>;
}

pub trait PrefetchableStreamExt: PrefetchableStream {
    fn into_boxed_stream(mut self) -> BoxedStream<Self::Item>
    where
        Self: 'static + Sized,
        Self::Item: Send,
    {
        let stream = stream! {
            while let Some(v) = self.fetch_next().await {
                yield v;
            }
        };

        // FIXME: Will this conversion to a stream introduce overhead?
        Box::new(Box::pin(stream))
    }

    fn filter_map<F, O>(self, f: F) -> FilterMap<Self, F>
    where
        F: FnMut(Self::Item) -> Option<O>,
        Self: Sized,
    {
        FilterMap { stream: self, f }
    }

    fn map<F, O>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> O,
        Self: Sized,
    {
        Map { stream: self, f }
    }
}

impl<T: ?Sized> PrefetchableStreamExt for T where T: PrefetchableStream {}

#[async_trait]
impl<T> PrefetchableStream for Box<dyn PrefetchableStream<Item = T>> {
    type Item = T;

    async fn start_prefetch(&mut self) {
        (**self).start_prefetch().await;
    }

    async fn fetch_next(&mut self) -> Option<T> {
        (**self).fetch_next().await
    }
}

/// The implementation for `filter_map` operator on the PrefetchableStream.
pub struct FilterMap<St, F> {
    stream: St,
    f: F,
}

#[async_trait]
impl<St, F, O> PrefetchableStream for FilterMap<St, F>
where
    St: PrefetchableStream,
    F: FnMut(St::Item) -> Option<O> + Send,
    O: Send,
{
    type Item = O;

    async fn start_prefetch(&mut self) {
        self.stream.start_prefetch().await;
    }

    async fn fetch_next(&mut self) -> Option<O> {
        loop {
            match self.stream.fetch_next().await {
                Some(v) => {
                    let filtered_batch = (self.f)(v);
                    if filtered_batch.is_some() {
                        return filtered_batch;
                    }
                    // If the filtered batch is none, just continue to fetch and
                    // filter until the underlying stream is exhausted.
                }
                None => return None,
            }
        }
    }
}

/// The implementation for `map` operator on the PrefetchableStream.
pub struct Map<St, F> {
    stream: St,
    f: F,
}

#[async_trait]
impl<St, F, O> PrefetchableStream for Map<St, F>
where
    St: PrefetchableStream,
    F: FnMut(St::Item) -> O + Send,
    O: Send,
{
    type Item = O;

    async fn start_prefetch(&mut self) {
        self.stream.start_prefetch().await;
    }

    async fn fetch_next(&mut self) -> Option<O> {
        self.stream.fetch_next().await.map(|v| (self.f)(v))
    }
}

/// A noop prefetcher.
///
/// A wrapper with a underlying stream without prefetch logic.
pub struct NoopPrefetcher<T>(pub BoxedStream<T>);

#[async_trait]
impl<T> PrefetchableStream for NoopPrefetcher<T> {
    type Item = T;

    async fn start_prefetch(&mut self) {
        // It's just a noop operation.
    }

    async fn fetch_next(&mut self) -> Option<T> {
        self.0.next().await
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn test_trait_object_prefetchable_stream() {
        let numbers = vec![1, 2, 3];
        let stream = stream::iter(numbers.clone());
        let stream = NoopPrefetcher(Box::new(stream));
        let mut stream: Box<dyn PrefetchableStream<Item = i32>> = Box::new(stream);

        let mut fetched_numbers = Vec::with_capacity(numbers.len());
        while let Some(v) = stream.fetch_next().await {
            fetched_numbers.push(v);
        }

        assert_eq!(numbers, fetched_numbers);
    }
}
