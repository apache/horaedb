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

use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
    path::Path,
};

use generic_error::BoxError;
use snafu::{OptionExt, ResultExt};
use tonic::service::Interceptor;

use crate::{
    auth::AUTHORIZATION,
    error::{Internal, InternalNoCause, Result},
};

#[derive(Debug, Clone, Default)]
pub struct AuthWithFile {
    enable: bool,
    file_path: String,
    // name -> password
    users: HashMap<String, String>,
}

impl AuthWithFile {
    pub fn new(enable: bool, file_path: String) -> Self {
        Self {
            enable,
            file_path,
            users: HashMap::new(),
        }
    }

    // Load a csv format config
    pub fn load_credential(&mut self) -> Result<()> {
        if !self.enable {
            return Ok(());
        }

        let path = Path::new(&self.file_path);
        if !path.exists() {
            return InternalNoCause {
                msg: format!("file not existed: {:?}", path),
            }
            .fail();
        }

        let file = File::open(path).box_err().context(Internal {
            msg: "failed to open file",
        })?;
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.box_err().context(Internal {
                msg: "failed to read line",
            })?;
            let (username, password) = line.split_once(',').with_context(|| InternalNoCause {
                msg: format!("invalid line: {:?}", line),
            })?;
            self.users
                .insert(username.to_string(), password.to_string());
        }

        Ok(())
    }

    // TODO: currently we only support basic auth
    // This function should return Result
    pub fn identify(&self, input: Option<String>) -> bool {
        if !self.enable {
            return true;
        }

        let input = match input {
            Some(v) => v,
            None => return false,
        };
        let input = match input.split_once("Basic ") {
            Some((_, encoded)) => match base64::decode(encoded) {
                Ok(v) => v,
                Err(_e) => return false,
            },
            None => return false,
        };
        let input = match std::str::from_utf8(&input) {
            Ok(v) => v,
            Err(_e) => return false,
        };
        match input.split_once(':') {
            Some((user, pass)) => self
                .users
                .get(user)
                .map(|expected| expected == pass)
                .unwrap_or_default(),
            None => false,
        }
    }
}

pub fn get_authorization<T>(req: &tonic::Request<T>) -> Option<String> {
    req.metadata()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok().map(String::from))
}

impl Interceptor for AuthWithFile {
    fn call(
        &mut self,
        request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        // TODO: extract username from request
        let authorization = get_authorization(&request);
        if self.identify(authorization) {
            Ok(request)
        } else {
            Err(tonic::Status::unauthenticated("unauthenticated"))
        }
    }
}
