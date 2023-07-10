// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{fmt, time::Duration};

#[derive(Clone)]
pub enum MetricOp {
    Add,
}

#[derive(Clone)]
pub struct MetricValue<T: Clone + fmt::Debug> {
    pub name: String,
    pub val: T,
    pub op: Option<MetricOp>,
}

#[derive(Clone)]
pub enum Metric {
    Boolean(MetricValue<bool>),
    Number(MetricValue<usize>),
    Duration(MetricValue<Duration>),
}

impl Metric {
    #[inline]
    pub fn number(name: String, val: usize, op: Option<MetricOp>) -> Self {
        Metric::Number(MetricValue { name, val, op })
    }

    #[inline]
    pub fn duration(name: String, val: Duration, op: Option<MetricOp>) -> Self {
        Metric::Duration(MetricValue { name, val, op })
    }

    #[inline]
    pub fn boolean(name: String, val: bool, op: Option<MetricOp>) -> Self {
        Metric::Boolean(MetricValue { name, val, op })
    }

    #[inline]
    pub fn name(&self) -> &str {
        match self {
            Self::Boolean(v) => &v.name,
            Self::Number(v) => &v.name,
            Self::Duration(v) => &v.name,
        }
    }

    #[inline]
    pub fn op(&self) -> &Option<MetricOp> {
        match self {
            Self::Boolean(v) => &v.op,
            Self::Number(v) => &v.op,
            Self::Duration(v) => &v.op,
        }
    }

    // Add performs value add when metrics are same type,
    // If their types are different, do nothing.
    #[inline]
    pub fn add(&mut self, rhs: &Self) {
        match (self, rhs) {
            (Self::Boolean(lhs), Self::Boolean(rhs)) => lhs.val |= rhs.val,
            (Self::Number(lhs), Self::Number(rhs)) => lhs.val += rhs.val,
            (Self::Duration(lhs), Self::Duration(rhs)) => lhs.val += rhs.val,
            _ => {}
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
        }
    }
}
