// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    error::Result as DfResult,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, StreamExt};

#[macro_export]
macro_rules! arrow_schema {
    ($(($field_name:expr, $data_type:ident)),* $(,)?) => {{
        let fields = vec![
            $(
                arrow::datatypes::Field::new($field_name, arrow::datatypes::DataType::$data_type, true),
            )*
        ];
        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }};
}

#[macro_export]
macro_rules! create_array {
    (Boolean, $values: expr) => {
        std::sync::Arc::new(arrow::array::BooleanArray::from($values))
    };
    (Int8, $values: expr) => {
        std::sync::Arc::new(arrow::array::Int8Array::from($values))
    };
    (Int16, $values: expr) => {
        std::sync::Arc::new(arrow::array::Int16Array::from($values))
    };
    (Int32, $values: expr) => {
        std::sync::Arc::new(arrow::array::Int32Array::from($values))
    };
    (Int64, $values: expr) => {
        std::sync::Arc::new(arrow::array::Int64Array::from($values))
    };
    (UInt8, $values: expr) => {
        std::sync::Arc::new(arrow::array::UInt8Array::from($values))
    };
    (UInt16, $values: expr) => {
        std::sync::Arc::new(arrow::array::UInt16Array::from($values))
    };
    (UInt32, $values: expr) => {
        std::sync::Arc::new(arrow::array::UInt32Array::from($values))
    };
    (UInt64, $values: expr) => {
        std::sync::Arc::new(arrow::array::UInt64Array::from($values))
    };
    (Float16, $values: expr) => {
        std::sync::Arc::new(arrow::array::Float16Array::from($values))
    };
    (Float32, $values: expr) => {
        std::sync::Arc::new(arrow::array::Float32Array::from($values))
    };
    (Float64, $values: expr) => {
        std::sync::Arc::new(arrow::array::Float64Array::from($values))
    };
    (Utf8, $values: expr) => {
        std::sync::Arc::new(arrow::array::StringArray::from($values))
    };
    (Binary, $values: expr) => {
        std::sync::Arc::new(arrow::array::BinaryArray::from_vec($values))
    };
}

/// Creates a record batch from literal slice of values, suitable for rapid
/// testing and development.
///
/// Example:
/// ```
/// let batch = record_batch!(
///     ("a", Int32, vec![1, 2, 3]),
///     ("b", Float64, vec![Some(4.0), None, Some(5.0)]),
///     ("c", Utf8, vec!["alpha", "beta", "gamma"])
/// );
/// ```
#[macro_export]
macro_rules! record_batch {
    ($(($name: expr, $type: ident, $values: expr)),*) => {
        {
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                $(
                    arrow::datatypes::Field::new($name, arrow::datatypes::DataType::$type, true),
                )*
            ]));

            let batch = arrow::array::RecordBatch::try_new(
                schema,
                vec![$(
                    $crate::create_array!($type, $values),
                )*]
            );

            batch
        }
    }
}

struct DequeBasedStream {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchStream for DequeBasedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for DequeBasedStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.batches.is_empty() {
            return Poll::Ready(None);
        }

        Poll::Ready(Some(Ok(self.batches.pop_front().unwrap())))
    }
}

/// Creates an record batch stream for testing purposes.
pub fn make_sendable_record_batches<I>(batches: I) -> SendableRecordBatchStream
where
    I: IntoIterator<Item = RecordBatch>,
{
    let batches = VecDeque::from_iter(batches);
    let schema = batches[0].schema();
    Box::pin(DequeBasedStream { batches, schema })
}

pub async fn check_stream<I>(mut stream: SendableRecordBatchStream, expected: I)
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut iter = expected.into_iter();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch, iter.next().unwrap());
    }
    assert!(iter.next().is_none());
}

mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn test_arrow_schema_macro() {
        let schema = arrow_schema![("a", UInt8), ("b", UInt8), ("c", UInt8), ("d", UInt8),];

        let expected_names = ["a", "b", "c", "d"];
        for (i, f) in schema.fields().iter().enumerate() {
            assert_eq!(f.name(), expected_names[i]);
        }
    }

    #[tokio::test]
    async fn test_sendable_record_batch() {
        let input = [
            record_batch!(("pk1", UInt8, vec![11, 11]), ("pk2", UInt8, vec![100, 100])).unwrap(),
            record_batch!(("pk1", UInt8, vec![22, 22]), ("pk2", UInt8, vec![200, 200])).unwrap(),
        ];
        let mut stream = make_sendable_record_batches(input.clone());
        let mut i = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            assert_eq!(batch, input[i]);
            i += 1;
        }
        assert_eq!(2, i);
    }
}
