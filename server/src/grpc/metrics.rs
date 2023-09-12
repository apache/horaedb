// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Grpc server metrics

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_histogram, register_histogram_vec, register_int_counter_vec,
    Histogram, HistogramVec, IntCounterVec,
};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

// Register auto flush static metrics.
make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        handle_route,
        handle_write,
        handle_sql_query,
        handle_prom_query,
        handle_stream_write,
        handle_stream_sql_query,
    }

    pub struct GrpcHandlerDurationHistogramVec: LocalHistogram {
        "type" => GrpcTypeKind,
    }

    pub label_enum RemoteEngineTypeKind {
        stream_read,
        write,
        get_table_info,
        write_batch,
        execute_physical_plan,
    }

    pub struct RemoteEngineGrpcHandlerDurationHistogramVec: LocalHistogram {
        "type" => RemoteEngineTypeKind,
    }

    pub label_enum RemoteEngineGrpcTypeKind {
        write_succeeded,
        write_failed,
        stream_query,
        stream_query_succeeded,
        stream_query_failed,
        write_succeeded_row,
        write_failed_row,
        query_succeeded_row,
        dedupped_stream_query,
    }

    pub struct RemoteEngineGrpcHandlerCounterVec: LocalIntCounter {
        "type" => RemoteEngineGrpcTypeKind,
    }

    pub label_enum MetaEventTypeKind {
        open_shard,
        close_shard,
        create_table_on_shard,
        drop_table_on_shard,
        open_table_on_shard,
        close_table_on_shard,
    }

    pub struct MetaEventGrpcHandlerDurationHistogramVec: LocalHistogram {
        "type" => MetaEventTypeKind,
    }
}

// Register global metrics.
lazy_static! {
    pub static ref GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL: HistogramVec =
        register_histogram_vec!(
            "grpc_handler_duration",
            "Bucketed histogram of grpc server handler",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL: HistogramVec =
        register_histogram_vec!(
            "remote_engine_grpc_handler_duration",
            "Bucketed histogram of remote engine grpc server handler",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC_GLOBAL: IntCounterVec =
        register_int_counter_vec!(
            "remote_engine_grpc_handler_counter",
            "Remote engine grpc handler counter",
            &["type"]
        )
        .unwrap();
    pub static ref REMOTE_ENGINE_WRITE_BATCH_NUM_ROWS_HISTOGRAM: Histogram = register_histogram!(
        "remote_engine_write_batch_num_rows",
        "Bucketed histogram of grpc server handler",
        vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 2000.0]
    )
    .unwrap();
    pub static ref META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL: HistogramVec =
        register_histogram_vec!(
            "meta_event_grpc_handler_duration",
            "Bucketed histogram of meta event grpc server handler",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap();
}

// Register thread local metrics with default flush interval (1s).
lazy_static! {
    pub static ref GRPC_HANDLER_DURATION_HISTOGRAM_VEC: GrpcHandlerDurationHistogramVec = auto_flush_from!(
        GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL,
        GrpcHandlerDurationHistogramVec
    );
    pub static ref REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC: RemoteEngineGrpcHandlerDurationHistogramVec = auto_flush_from!(
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL,
        RemoteEngineGrpcHandlerDurationHistogramVec
    );
    pub static ref REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC: RemoteEngineGrpcHandlerCounterVec = auto_flush_from!(
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC_GLOBAL,
        RemoteEngineGrpcHandlerCounterVec
    );
    pub static ref META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC: MetaEventGrpcHandlerDurationHistogramVec = auto_flush_from!(
        META_EVENT_GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL,
        MetaEventGrpcHandlerDurationHistogramVec
    );
}
