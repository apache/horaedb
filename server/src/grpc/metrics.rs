// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Grpc server metrics

use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram_vec, HistogramVec};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

// Register auto flush static metrics.
make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        handle_route,
        handle_write,
        handle_query,
        handle_stream_write,
        handle_stream_query,
    }

    pub struct GrpcHandlerDurationHistogramVec: LocalHistogram {
        "type" => GrpcTypeKind,
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
}

// Register thread local metrics with default flush interval (1s).
lazy_static! {
    pub static ref GRPC_HANDLER_DURATION_HISTOGRAM_VEC: GrpcHandlerDurationHistogramVec = auto_flush_from!(
        GRPC_HANDLER_DURATION_HISTOGRAM_VEC_GLOBAL,
        GrpcHandlerDurationHistogramVec
    );
}
