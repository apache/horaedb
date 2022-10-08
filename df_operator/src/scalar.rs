// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
