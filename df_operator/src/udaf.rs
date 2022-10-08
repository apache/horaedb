// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
