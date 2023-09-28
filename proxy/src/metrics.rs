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

// Grpc proxy metrics

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets,
    local::{LocalHistogram, LocalHistogramTimer},
    register_histogram, register_histogram_vec, register_int_counter_vec, Histogram, HistogramVec,
    IntCounterVec,
};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

// Proxy counters
make_auto_flush_static_metric! {
    pub label_enum GrpcTypeKind {
        write_succeeded,
        write_failed,
        incoming_query,
        query_succeeded,
        query_failed,
        route_succeeded,
        route_failed,
        stream_query_succeeded,
        stream_query_failed,
        stream_query,
        write_succeeded_row,
        write_failed_row,
        query_succeeded_row,
        query_affected_row,
        dedupped_stream_query,
    }

    pub struct GrpcHandlerCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
    }

    pub label_enum HttpTypeKind {
        write_failed,
        write_failed_row,
        incoming_prom_query,
        prom_query_succeeded,
        prom_query_failed,
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

// Histograms of proxy
lazy_static! {
    pub static ref DURATION_SINCE_QUERY_START_TIME: HistogramVec = register_histogram_vec!(
        "duration_since_query_start_time",
        "Duration since query start time, range:1h,2h,...,30d",
        &["table"],
        exponential_buckets(3600.0, 2.0, 10).unwrap()
    )
    .unwrap();
    pub static ref QUERY_TIME_RANGE: HistogramVec = register_histogram_vec!(
        "query_time_range",
        "Query time range, range:1h,2h,...,30d",
        &["table"],
        exponential_buckets(3600.0, 2.0, 10).unwrap()
    )
    .unwrap();
}
