// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Grpc proxy metrics

use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        write_succeeded,
        write_failed,
        query_succeeded,
        query_failed,
        query,
        stream_query_succeeded,
        stream_query_failed,
        stream_query,
        write_succeeded_row,
        write_failed_row,
        query_succeeded_row,
        query_affected_row,
    }

    pub struct GrpcHandlerCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
    }

    pub label_enum HttpTypeKind {
        write_failed,
        write_failed_row,
    }

    pub struct HttpHandlerCounterVec: LocalIntCounter {
        "type" => HttpTypeKind,
    }
}

lazy_static! {
    pub static ref GRPC_HANDLER_COUNTER_VEC_GLOBAL: IntCounterVec =
        register_int_counter_vec!("grpc_handler_counter", "Grpc handler counter", &["type"])
            .unwrap();
    pub static ref HTTP_HANDLER_COUNTER_VEC_GLOBAL: IntCounterVec =
        register_int_counter_vec!("http_handler_counter", "Http handler counter", &["type"])
            .unwrap();
}

lazy_static! {
    pub static ref GRPC_HANDLER_COUNTER_VEC: GrpcHandlerCounterVec =
        auto_flush_from!(GRPC_HANDLER_COUNTER_VEC_GLOBAL, GrpcHandlerCounterVec);
    pub static ref HTTP_HANDLER_COUNTER_VEC: HttpHandlerCounterVec =
        auto_flush_from!(HTTP_HANDLER_COUNTER_VEC_GLOBAL, HttpHandlerCounterVec);
}
