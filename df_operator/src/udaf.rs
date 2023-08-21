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

//! UDAF support.

use std::sync::Arc;

use datafusion::physical_plan::udaf::AggregateUDF;

use crate::functions::AggregateFunction;

/// Logical representation of a UDAF.
#[derive(Debug, Clone)]
pub struct AggregateUdf {
    /// DataFusion UDAF.
    df_udaf: Arc<AggregateUDF>,
}

impl AggregateUdf {
    pub fn create(name: &str, func: AggregateFunction) -> Self {
        let signature = func.signature().to_datafusion_signature();
        let return_type = func.return_type().to_datafusion_return_type();
        let accumulator = func.to_datafusion_accumulator();
        let state_type = func.to_datafusion_state_type();

        let df_udaf = Arc::new(AggregateUDF::new(
            name,
            &signature,
            &return_type,
            &accumulator,
            &state_type,
        ));

        Self { df_udaf }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.df_udaf.name
    }

    #[inline]
    pub fn to_datafusion_udaf(&self) -> Arc<AggregateUDF> {
        self.df_udaf.clone()
    }
}
