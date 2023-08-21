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

// Our backtrace is defined like this
// #[snafu(display("Time range is not found.\nBacktrace\n:{}", backtrace))]
//
// So here we split by `Backtrace`, and return first part
pub fn remove_backtrace_from_err(err_string: &str) -> &str {
    err_string
        .split("Backtrace")
        .next()
        .map(|s| s.trim_end())
        .unwrap_or(err_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_backtrace() {
        let cases = vec![
            (
                r#"Failed to execute select, err:Failed to execute logical plan, err:Failed to do physical optimization,
err:DataFusion Failed to optimize physical plan, err:Error during planning.
Backtrace:
 0 <snafu::backtrace_shim::Backtrace as snafu::GenerateBacktrace>::generate::h996ee016dfa35e37"#,
                r#"Failed to execute select, err:Failed to execute logical plan, err:Failed to do physical optimization,
err:DataFusion Failed to optimize physical plan, err:Error during planning."#,
            ),
            ("", ""),
            ("some error", "some error"),
        ];

        for (input, expected) in cases {
            assert_eq!(expected, remove_backtrace_from_err(input));
        }
    }
}
