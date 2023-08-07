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

//! Schema provider based on cluster.

use cluster::{config::SchemaConfig, ClusterRef};

use crate::schema_config_provider::{Result, SchemaConfigProvider};

pub struct ClusterBasedProvider {
    #[allow(dead_code)]
    cluster: ClusterRef,
    default_schema_config: SchemaConfig,
}

impl ClusterBasedProvider {
    pub fn new(cluster: ClusterRef) -> Self {
        Self {
            cluster,
            default_schema_config: Default::default(),
        }
    }
}

impl SchemaConfigProvider for ClusterBasedProvider {
    fn schema_config(&self, _schema_name: &str) -> Result<Option<&SchemaConfig>> {
        // FIXME: Fetch the schema config from the cluster rather than the hard-coded
        // default_schema_config.
        Ok(Some(&self.default_schema_config))
    }
}
