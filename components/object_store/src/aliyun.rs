// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt::Display, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use oss_rust_sdk::{async_object::AsyncObjectAPI, errors::Error as AliyunError, prelude::OSS};
use snafu::{ResultExt, Snafu};
use upstream::{
    path::Path, Error as OssError, GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to put object at path: {}, err: {}", path, source))]
    PutObject { path: String, source: AliyunError },

    #[snafu(display("Failed to get object at path: {}, err: {}", path, source))]
    GetObject { path: String, source: AliyunError },

    #[snafu(display("Failed to head object at path: {}, err: {}", path, source))]
    HeadObject { path: String, source: AliyunError },

    #[snafu(display("Failed to delete object at path: {}, err: {}", path, source))]
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
    pub fn new(
        key_id: impl Into<String>,
        key_secret: impl Into<String>,
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        let oss = OSS::new(
            key_id.into(),
            key_secret.into(),
            endpoint.into(),
            bucket.into(),
        );
        Self { oss }
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
            .context(PutObject {
                path: &location.to_string(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let bytes = self
            .oss
            .get_object(&location.to_string(), None::<HashMap<String, String>>, None)
            .await
            .context(GetObject {
                path: &location.to_string(),
            })?;

        Ok(GetResult::Stream(stream::once(async { Ok(bytes) }).boxed()))
    }

    async fn get_range(&self, _location: &Path, _range: Range<usize>) -> Result<Bytes> {
        Err(Error::Unimplemented {
            op: "get_range".to_string(),
        }
        .into())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let head = self
            .oss
            .head_object(&location.to_string())
            .await
            .context(HeadObject {
                path: &location.to_string(),
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
            .context(DeleteObject {
                path: &location.to_string(),
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
}

impl Display for AliyunOSS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AliyunOSS({})", self.oss.bucket())
    }
}
