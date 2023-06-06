// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

// Grpc proxy metrics

use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        write_failed,
    }

    pub struct GrpcHandlerCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
    }
}

lazy_static! {
    pub static ref GRPC_HANDLER_COUNTER_VEC_GLOBAL: IntCounterVec =
        register_int_counter_vec!("grpc_handler_counter", "Grpc handler counter", &["type"])
            .unwrap();
}

lazy_static! {
    pub static ref GRPC_HANDLER_COUNTER_VEC: GrpcHandlerCounterVec =
        auto_flush_from!(GRPC_HANDLER_COUNTER_VEC_GLOBAL, GrpcHandlerCounterVec);
}
