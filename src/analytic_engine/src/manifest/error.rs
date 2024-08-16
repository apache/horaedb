// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use thiserror::Error;

use crate::ErrorKind;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] InnerError);

impl From<anyhow::Error> for Error {
    fn from(source: anyhow::Error) -> Self {
        Self(InnerError::Other { source })
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self.0 {
            InnerError::Other { .. } => ErrorKind::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum InnerError {
    #[error(transparent)]
    Other {
        #[from]
        source: anyhow::Error,
    },
}
