// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt, time::Duration};

#[derive(Clone)]
pub struct MetricValue<T: Clone + fmt::Debug> {
    pub name: String,
    pub val: T,
}

#[derive(Clone, Debug)]
pub enum Metric {
    Boolean(MetricValue<bool>),
    Counter(MetricValue<usize>),
    Elapsed(MetricValue<Duration>),
}

impl Metric {
    #[inline]
    pub fn counter(name: String, val: usize) -> Self {
        Metric::Counter(MetricValue { name, val })
    }

    #[inline]
    pub fn elapsed(name: String, val: Duration) -> Self {
        Metric::Elapsed(MetricValue { name, val })
    }

    #[inline]
    pub fn boolean(name: String, val: bool) -> Self {
        Metric::Boolean(MetricValue { name, val })
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for MetricValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={:?}", self.name, self.val)
    }
}
