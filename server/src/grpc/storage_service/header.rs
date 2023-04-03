// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use log::warn;
use tonic::metadata::{KeyAndValueRef, MetadataMap};

/// Rpc request header
/// Tenant/token will be saved in header in future
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct RequestHeader {
    metas: HashMap<String, Vec<u8>>,
}

impl From<&MetadataMap> for RequestHeader {
    fn from(meta: &MetadataMap) -> Self {
        let metas = meta
            .iter()
            .filter_map(|kv| match kv {
                KeyAndValueRef::Ascii(key, val) => {
                    // TODO: The value may be encoded in base64, which is not expected.
                    Some((key.to_string(), val.as_encoded_bytes().to_vec()))
                }
                KeyAndValueRef::Binary(key, val) => {
                    warn!(
                        "Binary header is not supported yet and will be omit, key:{:?}, val:{:?}",
                        key, val
                    );
                    None
                }
            })
            .collect();

        Self { metas }
    }
}

impl RequestHeader {
    #[allow(dead_code)]
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.metas.get(key).map(|v| v.as_slice())
    }
}
