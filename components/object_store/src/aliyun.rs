// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt::Display, ops::Range};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use oss_rust_sdk::{async_object::AsyncObjectAPI, errors::Error as AliyunError, prelude::OSS};
use oss_rust_sdk::oss::Options;
use snafu::{ResultExt, Snafu};
use tokio::io::AsyncWrite;
use upstream::{
    path::Path, Error as OssError, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    Result,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to put object at path:{}, err:{}", path, source))]
    PutObject { path: String, source: AliyunError },

    #[snafu(display("Failed to get object at path:{}, err:{}", path, source))]
    GetObject { path: String, source: AliyunError },

    #[snafu(display("Failed to get range of object at path:{}, err:{}", path, source))]
    GetRangeObject {
        path: String,
        range: Range<usize>,
        source: AliyunError,
    },

    #[snafu(display("Failed to head object at path:{}, err:{}", path, source))]
    HeadObject { path: String, source: AliyunError },

    #[snafu(display("Failed to delete object at path:{}, err:{}", path, source))]
    DeleteObject { path: String, source: AliyunError },

    #[snafu(display("Operation {} is not implemented", op))]
    Unimplemented { op: String },
}

impl From<Error> for OssError {
    fn from(source: Error) -> Self {
        Self::Generic {
            store: "Aliyun",
            source: Box::new(source),
        }
    }
}

#[derive(Debug)]
pub struct AliyunOSS {
    oss: OSS<'static>,
}

impl AliyunOSS {
    const RANGE_KEY: &str = "Range";

    pub fn new(
        key_id: impl Into<String>,
        key_secret: impl Into<String>,
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        pool_max_idle_per_host: impl Into<usize>,
        timeout: impl Into<Duration>,
    ) -> Self {
        let oss = OSS::new_with_opts(
            key_id.into(),
            key_secret.into(),
            endpoint.into(),
            bucket.into(),
            Options {
                pool_max_idle_per_host: Some(pool_max_idle_per_host.into()),
                timeout: Some(timeout.into()),
            },
        );
        Self { oss }
    }

    fn make_range_header(range: &Range<usize>, headers: &mut HashMap<String, String>) {
        assert!(!range.is_empty());
        let range_value = format!("bytes={}-{}", range.start, range.end - 1);

        headers.insert(Self::RANGE_KEY.to_string(), range_value);
    }
}

#[async_trait]
impl ObjectStore for AliyunOSS {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.oss
            .put_object(
                &bytes,
                &location.to_string(),
                None::<HashMap<String, String>>,
                None,
            )
            .await
            .with_context(|| PutObject {
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(Error::Unimplemented {
            op: "put_multipart".to_string(),
        }
        .into())
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(Error::Unimplemented {
            op: "abort_multipart".to_string(),
        }
        .into())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let bytes = self
            .oss
            .get_object(&location.to_string(), None::<HashMap<String, String>>, None)
            .await
            .with_context(|| GetObject {
                path: location.to_string(),
            })?;

        Ok(GetResult::Stream(stream::once(async { Ok(bytes) }).boxed()))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let mut headers = HashMap::new();
        Self::make_range_header(&range, &mut headers);

        let bytes = self
            .oss
            .get_object(&location.to_string(), Some(headers), None)
            .await
            .with_context(|| GetRangeObject {
                path: location.to_string(),
                range,
            })?;

        Ok(bytes)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let head = self
            .oss
            .head_object(&location.to_string())
            .await
            .with_context(|| HeadObject {
                path: location.to_string(),
            })?;

        Ok(ObjectMeta {
            last_modified: head.last_modified.into(),
            size: head.size,
            location: location.clone(),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.oss
            .delete_object(&location.to_string())
            .await
            .with_context(|| DeleteObject {
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        Err(Error::Unimplemented {
            op: "list".to_string(),
        }
        .into())
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::Unimplemented {
            op: "list_with_delimiter".to_string(),
        }
        .into())
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::Unimplemented {
            op: "copy".to_string(),
        }
        .into())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::Unimplemented {
            op: "copy_if_not_exists".to_string(),
        }
        .into())
    }
}

impl Display for AliyunOSS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AliyunOSS({})", self.oss.bucket())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_header() {
        let testcases = vec![
            ((0..500), "bytes=0-499"),
            ((1000..1001), "bytes=1000-1000"),
            ((200..1000), "bytes=200-999"),
        ];

        for (input_range, expect_value) in testcases {
            let mut headers = HashMap::new();
            AliyunOSS::make_range_header(&input_range, &mut headers);

            assert_eq!(headers.len(), 1);
            let range_value = headers
                .get(AliyunOSS::RANGE_KEY)
                .expect("should have range key");
            assert_eq!(range_value, expect_value);
        }
    }

    #[test]
    #[should_panic]
    fn test_panic_invalid_range_header() {
        let mut headers = HashMap::new();
        #[allow(clippy::reversed_empty_ranges)]
        AliyunOSS::make_range_header(&(500..499), &mut headers);
    }

    #[test]
    #[should_panic]
    fn test_panic_empty_range_header() {
        let mut headers = HashMap::new();
        AliyunOSS::make_range_header(&(500..500), &mut headers);
    }
}
