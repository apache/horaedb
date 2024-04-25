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

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] InnerError);

impl From<anyhow::Error> for Error {
    fn from(source: anyhow::Error) -> Self {
        Self(InnerError::Other { source })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    KeyTooLarge,
    Internal,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self.0 {
            InnerError::KeyTooLarge { .. } => ErrorKind::KeyTooLarge,
            InnerError::Other { .. } => ErrorKind::Internal,
        }
    }
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
}

#[derive(Error, Debug)]
pub(crate) enum InnerError {
    #[error("too large key, max:{max}, current:{current}")]
    KeyTooLarge { current: usize, max: usize },

    #[error(transparent)]
    Other {
        #[from]
        source: anyhow::Error,
    },
}
