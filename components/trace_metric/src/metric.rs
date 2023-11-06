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

use std::{fmt, time::Duration};

#[derive(Clone)]
pub enum MetricAggregator {
    Sum,
}

#[derive(Clone)]
pub struct MetricValue<T: Clone + fmt::Debug> {
    pub name: String,
    pub val: T,
    pub aggregator: Option<MetricAggregator>,
}

#[derive(Clone)]
pub enum Metric {
    Boolean(MetricValue<bool>),
    Number(MetricValue<usize>),
    Duration(MetricValue<Duration>),
    String(MetricValue<String>),
}

impl Metric {
    #[inline]
    pub fn number(name: String, val: usize, aggregator: Option<MetricAggregator>) -> Self {
        Metric::Number(MetricValue {
            name,
            val,
            aggregator,
        })
    }

    #[inline]
    pub fn duration(name: String, val: Duration, aggregator: Option<MetricAggregator>) -> Self {
        Metric::Duration(MetricValue {
            name,
            val,
            aggregator,
        })
    }

    #[inline]
    pub fn boolean(name: String, val: bool, aggregator: Option<MetricAggregator>) -> Self {
        Metric::Boolean(MetricValue {
            name,
            val,
            aggregator,
        })
    }

    #[inline]
    pub fn string(name: String, val: String, aggregator: Option<MetricAggregator>) -> Self {
        Metric::String(MetricValue {
            name,
            val,
            aggregator,
        })
    }

    #[inline]
    pub fn name(&self) -> &str {
        match self {
            Self::Boolean(v) => &v.name,
            Self::Number(v) => &v.name,
            Self::Duration(v) => &v.name,
            Self::String(v) => &v.name,
        }
    }

    #[inline]
    pub fn aggregator(&self) -> &Option<MetricAggregator> {
        match self {
            Self::Boolean(v) => &v.aggregator,
            Self::Number(v) => &v.aggregator,
            Self::Duration(v) => &v.aggregator,
            Self::String(v) => &v.aggregator,
        }
    }

    // Sum metric values together when metrics are same type,
    // Panic if their types are different.
    #[inline]
    pub fn sum(&mut self, rhs: &Self) {
        match (self, rhs) {
            (Self::Boolean(lhs), Self::Boolean(rhs)) => lhs.val |= rhs.val,
            (Self::Number(lhs), Self::Number(rhs)) => lhs.val += rhs.val,
            (Self::Duration(lhs), Self::Duration(rhs)) => lhs.val += rhs.val,
            (lhs, rhs) => {
                panic!("Only same type metric could be applied, lhs:{lhs:?}, rhs:{rhs:?}")
            }
        }
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for MetricValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={:?}", self.name, self.val)
    }
}

impl fmt::Debug for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Metric::Boolean(v) => write!(f, "{}={:?}", v.name, v.val),
            Metric::Number(v) => write!(f, "{}={:?}", v.name, v.val),
            Metric::Duration(v) => write!(f, "{}={:?}", v.name, v.val),
            Metric::String(v) => write!(f, "{}", v.val),
        }
    }
}
