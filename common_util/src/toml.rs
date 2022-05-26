// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Toml config utilities.

use std::{fs::File, io::Read};

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
pub fn parse_toml_from_path<'a, T>(path: &str, toml_buf: &'a mut String) -> Result<T>
where
    T: de::Deserialize<'a>,
{
    let mut file = File::open(path).context(OpenFile { path })?;
    file.read_to_string(toml_buf).context(ReadToml { path })?;

    toml::from_str(toml_buf).context(ParseToml { path })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_derive::Deserialize;
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
