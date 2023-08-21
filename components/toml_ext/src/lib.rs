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

//! Toml config utilities.

use std::{fs::File, io::Read};

use macros::define_result;
use serde::de;
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to open file, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    OpenFile {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to read toml, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    ReadToml {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse toml, path:{}, err:{}.\nBacktrace:\n{}",
        path,
        source,
        backtrace
    ))]
    ParseToml {
        path: String,
        source: toml::de::Error,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Read toml file from given `path` to `toml_buf`, then parsed it to `T` and
/// return.
pub fn parse_toml_from_path<T>(path: &str, toml_buf: &mut String) -> Result<T>
where
    T: de::DeserializeOwned,
{
    let mut file = File::open(path).context(OpenFile { path })?;
    file.read_to_string(toml_buf).context(ReadToml { path })?;

    toml::from_str(toml_buf).context(ParseToml { path })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde::Deserialize;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_parse_toml_from_path() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.toml");
        let path = file_path.to_str().unwrap();

        let mut f = File::create(path).expect("Failed to create test config file");
        f.write_all(b"host=\"localhost\"\nport=1081")
            .expect("Failed to write test config");

        f.sync_all().expect("Failed to sync test config");

        #[derive(Clone, Debug, Deserialize)]
        struct TestConfig {
            host: String,
            port: u16,
        }
        let mut config = TestConfig {
            host: "".to_string(),
            port: 0,
        };

        assert_eq!("", config.host);
        assert_eq!(0, config.port);

        let mut toml_str = String::new();

        config = parse_toml_from_path(path, &mut toml_str).unwrap();

        assert_eq!("localhost", config.host);
        assert_eq!(1081, config.port);
    }
}
