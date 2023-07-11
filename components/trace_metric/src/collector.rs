// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::metric::{Metric, MetricAggregator};

/// A collector for metrics of a single read request.
///
/// It can be cloned and shared among threads.
#[derive(Clone, Debug, Default)]
pub struct MetricsCollector {
    name: String,
    metrics: Arc<Mutex<Vec<Metric>>>,
    children: Arc<Mutex<Vec<MetricsCollector>>>,
}

impl MetricsCollector {
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
    pub fn span(&self, name: String) -> MetricsCollector {
        let mut children = self.children.lock().unwrap();
        let child = Self::new(name);
        children.push(child.clone());
        child
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Calls a closure on each top-level metrics of this collector.
    pub fn for_each_metric(&self, f: &mut impl FnMut(&Metric)) {
        let metrics = self.metrics.lock().unwrap();

        let mut metrics_by_name = BTreeMap::new();
        for metric in metrics.iter() {
            metrics_by_name
                .entry(metric.name())
                .or_insert_with(Vec::new)
                .push(metric);
        }

        for metrics in metrics_by_name.values() {
            if metrics.is_empty() {
                continue;
            }

            if let Some(op) = metrics[0].aggregator() {
                match op {
                    MetricAggregator::Sum => {
                        let mut first = metrics[0].clone();
                        for m in &metrics[1..] {
                            first.sum(m);
                        }
                        // only apply fn to first metric.
                        f(&first);
                    }
                }
            } else {
                for metric in metrics {
                    f(metric);
                }
            }
        }
    }

    /// Visit all the collectors including itself and its children.
    pub fn visit(&self, visitor: &mut impl CollectorVisitor) {
        self.visit_with_level(0, visitor);
    }

    /// Visit all the collectors including itself and its children.
    fn visit_with_level(&self, level: usize, visitor: &mut impl CollectorVisitor) {
        visitor.visit(level, self);
        // Clone the children to avoid holding the lock, which may cause deadlocks
        // because the lock order is not guaranteed.
        let children = self.children.lock().unwrap().clone();
        for child in children {
            child.visit_with_level(level + 1, visitor);
        }
    }
}

pub trait CollectorVisitor {
    fn visit(&mut self, level: usize, collector: &MetricsCollector);
}

#[derive(Default)]
pub struct FormatCollectorVisitor {
    buffer: String,
}

impl FormatCollectorVisitor {
    pub fn into_string(self) -> String {
        self.buffer
    }

    fn indent(level: usize) -> String {
        " ".repeat(level * 4)
    }

    fn append_line(&mut self, indent: &str, line: &str) {
        self.buffer.push_str(&format!("{indent}{line}\n"));
    }
}

impl CollectorVisitor for FormatCollectorVisitor {
    fn visit(&mut self, level: usize, collector: &MetricsCollector) {
        let collector_indent = Self::indent(level);
        self.append_line(&collector_indent, &format!("{}:", collector.name()));
        let metric_indent = Self::indent(level + 1);
        collector.for_each_metric(&mut |metric| {
            self.append_line(&metric_indent, &format!("{metric:?}"));
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new("root".to_string());
        collector.collect(Metric::number("counter".to_string(), 1, None));
        collector.collect(Metric::duration(
            "elapsed".to_string(),
            Duration::from_millis(100),
            None,
        ));
        let child_1_0 = collector.span("child_1_0".to_string());
        child_1_0.collect(Metric::boolean("boolean".to_string(), false, None));

        let child_2_0 = child_1_0.span("child_2_0".to_string());
        child_2_0.collect(Metric::number("counter".to_string(), 1, None));
        child_2_0.collect(Metric::duration(
            "elapsed".to_string(),
            Duration::from_millis(100),
            None,
        ));

        let child_1_1 = collector.span("child_1_1".to_string());
        child_1_1.collect(Metric::boolean("boolean".to_string(), false, None));
        let _child_1_2 = collector.span("child_1_2".to_string());

        let mut visitor = FormatCollectorVisitor::default();
        collector.visit(&mut visitor);
        let expect_output = r#"root:
    counter=1
    elapsed=100ms
    child_1_0:
        boolean=false
        child_2_0:
            counter=1
            elapsed=100ms
    child_1_1:
        boolean=false
    child_1_2:
"#;
        assert_eq!(expect_output, &visitor.into_string());
    }
}
