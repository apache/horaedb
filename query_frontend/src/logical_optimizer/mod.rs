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

//! Logical optimizer

mod type_conversion;
use std::sync::Arc;

use datafusion::{
    error::Result,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    optimizer::analyzer::Analyzer,
    prelude::SessionConfig,
};
use type_conversion::TypeConversion;

pub fn optimize_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let state = SessionState::with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
    let state = register_analyzer_rules(state);
    let plan = state.optimize(plan)?;

    Ok(plan)
}

fn register_analyzer_rules(mut state: SessionState) -> SessionState {
    // Our analyzer has high priority, so first add we custom rules, then add the
    // default ones.
    state = state.with_analyzer_rules(vec![Arc::new(crate::logical_optimizer::TypeConversion)]);
    for rule in Analyzer::new().rules {
        state = state.add_analyzer_rule(rule);
    }

    state
}
