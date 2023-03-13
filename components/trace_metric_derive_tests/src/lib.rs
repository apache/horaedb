// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use trace_metric::{MetricsCollector, TracedMetrics};

#[derive(Debug, Clone, TracedMetrics)]
pub struct ExampleMetrics {
    #[metric(counter)]
    pub counter: usize,
    #[metric(elapsed)]
    pub elapsed: Duration,
    #[metric(boolean)]
    pub boolean: bool,
    pub foo: String,

    #[metric(collector)]
    pub collector: MetricsCollector,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let collector = MetricsCollector::new("test".to_string());
        {
            let _ = ExampleMetrics {
                counter: 1,
                elapsed: Duration::from_secs(1),
                boolean: true,
                foo: "bar".to_owned(),
                collector: collector.clone(),
            };
        }

        let mut metric_num = 0;
        collector.visit_metrics(&mut |_| {
            metric_num += 1;
        });

        assert_eq!(metric_num, 3)
    }
}
