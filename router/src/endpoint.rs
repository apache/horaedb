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

//! Endpoint definition

use std::str::FromStr;

use ceresdbproto::storage;
use generic_error::GenericError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash, Serialize)]
pub struct Endpoint {
    pub addr: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(addr: String, port: u16) -> Self {
        Self { addr, port }
    }
}

impl ToString for Endpoint {
    fn to_string(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

impl FromStr for Endpoint {
    type Err = GenericError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (addr, raw_port) = match s.rsplit_once(':') {
            Some(v) => v,
            None => {
                let err_msg = "Can't find ':' in the source string".to_string();
                return Err(Self::Err::from(err_msg));
            }
        };
        let port = raw_port.parse().map_err(|e| {
            let err_msg = format!("Fail to parse port:{raw_port}, err:{e}");
            Self::Err::from(err_msg)
        })?;

        Ok(Endpoint {
            addr: addr.to_string(),
            port,
        })
    }
}

impl From<Endpoint> for storage::Endpoint {
    fn from(endpoint: Endpoint) -> Self {
        storage::Endpoint {
            ip: endpoint.addr,
            port: endpoint.port as u32,
        }
    }
}

impl From<storage::Endpoint> for Endpoint {
    fn from(endpoint_pb: storage::Endpoint) -> Self {
        Endpoint {
            addr: endpoint_pb.ip,
            port: endpoint_pb.port as u16,
        }
    }
}
