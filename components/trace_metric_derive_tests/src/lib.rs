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

use std::time::Duration;

use trace_metric::{MetricsCollector, TraceMetricWhenDrop};

#[derive(Debug, Clone, TraceMetricWhenDrop)]
pub struct ExampleMetrics {
    #[metric(number, sum)]
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
        let actual = formatter.into_string();

        let expected = r#"test:
    boolean=true
    boolean=false
    counter=11
    elapsed=1s
    elapsed=2s
"#;
        assert_eq!(expected, actual);
    }
}
