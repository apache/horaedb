// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Grpc server metrics

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_histogram_vec, register_int_counter_vec, HistogramVec,
    IntCounterVec,
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
