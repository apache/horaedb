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

//! The proxy module provides features such as forwarding and authentication,
//! adapts to different protocols.

use std::{collections::HashMap, fs::File, io, io::BufRead, path::Path};

use snafu::ResultExt;

use crate::auth::{Auth, FileNotExisted, OpenFile, ReadLine, Result, ADMIN_TENANT};

pub struct AuthWithFile {
    file_path: String,
    users: HashMap<String, String>,
}

impl AuthWithFile {
    pub fn new(file_path: String) -> Self {
        Self {
            file_path,
            users: HashMap::new(),
        }
    }
}

impl Auth for AuthWithFile {
    /// Load credential from file
    fn load_credential(&mut self) -> Result<()> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return FileNotExisted {
                path: self.file_path.clone(),
            }
            .fail();
        }

        let file = File::open(path).context(OpenFile)?;
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.context(ReadLine)?;
            if let Some((value, key)) = line.split_once(':') {
                self.users.insert(key.to_string(), value.to_string());
            }
        }

        Ok(())
    }

    fn identify(&self, tenant: Option<String>, token: Option<String>) -> bool {
        if let Some(tenant) = tenant {
            if tenant == ADMIN_TENANT {
                return true;
            }
        }

        match token {
            Some(token) => self.users.contains_key(&token),
            None => false,
        }
    }
}
