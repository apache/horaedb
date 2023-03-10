// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use crate::metric::Metric;

/// A collector for metrics of a single read request.
///
/// It can be cloned and shared among threads.
#[derive(Clone, Debug)]
pub struct Collector {
    name: String,
    metrics: Arc<Mutex<Vec<Metric>>>,
    children: Arc<Mutex<Vec<Collector>>>,
}

impl Collector {
    /// Create a new collector with the given name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            metrics: Arc::new(Mutex::new(vec![])),
            children: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Collect a metric.
    pub fn collect(&self, metric: Metric) {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.push(metric);
    }

    /// Span a child collector with a given name.
    pub fn span(&self, name: String) -> Collector {
        let mut children = self.children.lock().unwrap();
        let child = Self::new(name);
        children.push(child.clone());
        child
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Visit all metrics in the collector, excluding the metrics belonging to
    /// the children.
    pub fn visit_metrics(&self, f: &mut dyn FnMut(&Metric)) {
        let metrics = self.metrics.lock().unwrap();
        for metric in metrics.iter() {
            f(metric);
        }
    }

    /// Visit all the collectors including itself and its children.
    pub fn visit(&self, f: &mut dyn FnMut(&Collector)) {
        f(self);
        let children = self.children.lock().unwrap();
        for child in children.iter() {
            child.visit(f);
        }
    }
}
