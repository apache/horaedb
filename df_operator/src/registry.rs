// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Function registry.

use std::{collections::HashMap, sync::Arc};

use common_util::define_result;
use snafu::{ensure, Backtrace, Snafu};

use crate::{scalar::ScalarUdf, udaf::AggregateUdf, udfs};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Udf already exists, name:{}.\nBacktrace:\n{}", name, backtrace))]
    UdfExists { name: String, backtrace: Backtrace },
}

define_result!(Error);

/// A registry knows how to build logical expressions out of user-defined
/// function' names
pub trait FunctionRegistry {
    fn register_udf(&mut self, udf: ScalarUdf) -> Result<()>;

    fn register_udaf(&mut self, udaf: AggregateUdf) -> Result<()>;

    fn find_udf(&self, name: &str) -> Result<Option<ScalarUdf>>;

    fn find_udaf(&self, name: &str) -> Result<Option<AggregateUdf>>;

    fn list_udfs(&self) -> Result<Vec<ScalarUdf>>;
}

/// Default function registry.
#[derive(Debug, Default)]
pub struct FunctionRegistryImpl {
    scalar_functions: HashMap<String, ScalarUdf>,
    aggregate_functions: HashMap<String, AggregateUdf>,
}

impl FunctionRegistryImpl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load all provided udfs.
    pub fn load_functions(&mut self) -> Result<()> {
        udfs::register_all_udfs(self)
    }
}

impl FunctionRegistry for FunctionRegistryImpl {
    fn register_udf(&mut self, udf: ScalarUdf) -> Result<()> {
        ensure!(
            !self.scalar_functions.contains_key(udf.name()),
            UdfExists { name: udf.name() }
        );

        self.scalar_functions.insert(udf.name().to_string(), udf);

        Ok(())
    }

    fn register_udaf(&mut self, udaf: AggregateUdf) -> Result<()> {
        ensure!(
            !self.aggregate_functions.contains_key(udaf.name()),
            UdfExists { name: udaf.name() }
        );

        self.aggregate_functions
            .insert(udaf.name().to_string(), udaf);

        Ok(())
    }

    fn find_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        let udf = self.scalar_functions.get(name).cloned();
        Ok(udf)
    }

    fn find_udaf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        let udaf = self.aggregate_functions.get(name).cloned();
        Ok(udaf)
    }

    fn list_udfs(&self) -> Result<Vec<ScalarUdf>> {
        let udfs = self.scalar_functions.values().cloned().collect();
        Ok(udfs)
    }
}

pub type FunctionRegistryRef = Arc<dyn FunctionRegistry + Send + Sync>;
