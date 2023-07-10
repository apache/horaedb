// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

#[derive(Debug, Clone, TraceMetricWhenDrop)]
pub struct ExampleMetrics {
    #[metric(number, add)]
    pub counter: usize,
    #[metric(duration)]
    pub elapsed: Duration,
    #[metric(boolean)]
    pub boolean: bool,
    pub foo: String,

    #[metric(collector)]
    pub collector: MetricsCollector,
}

#[cfg(test)]
mod test {
    use trace_metric::collector::FormatCollectorVisitor;

    use super::*;

    #[test]
    fn basic() {
        let collector = MetricsCollector::new("test".to_string());
        {
            let _ = ExampleMetrics {
                counter: 1,
                elapsed: Duration::from_secs(1),
                boolean: true,
                foo: "foor".to_owned(),
                collector: collector.clone(),
            };
            let _ = ExampleMetrics {
                counter: 10,
                elapsed: Duration::from_secs(2),
                boolean: false,
                foo: "bar".to_owned(),
                collector: collector.clone(),
            };
        }
        let mut formatter = FormatCollectorVisitor::default();
        collector.visit(&mut formatter);
        let expect_output = r#"test:
    counter=11
    boolean=true
    boolean=false
    elapsed=1s
    elapsed=2s
"#;

        assert_eq!(expect_output, &formatter.into_string());
    }
}
