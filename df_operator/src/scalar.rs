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

//! Scalar udfs.

use std::sync::Arc;

use datafusion::physical_plan::udf::ScalarUDF;

use crate::functions::ScalarFunction;

/// Logical representation of a UDF.
#[derive(Debug, Clone)]
pub struct ScalarUdf {
    /// DataFusion UDF.
    df_udf: Arc<ScalarUDF>,
}

impl ScalarUdf {
    pub fn create(name: &str, func: ScalarFunction) -> Self {
        let signature = func.signature().to_datafusion_signature();
        let return_type = func.return_type().to_datafusion_return_type();
        let scalar_fn = func.to_datafusion_function();

        let df_udf = Arc::new(ScalarUDF::new(name, &signature, &return_type, &scalar_fn));

        Self { df_udf }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.df_udf.name
    }

    /// Convert into datafusion's udf
    #[inline]
    pub fn to_datafusion_udf(&self) -> Arc<ScalarUDF> {
        self.df_udf.clone()
    }
}
