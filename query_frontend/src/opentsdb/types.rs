use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::plan::Plan;

#[derive(Serialize, Deserialize, Debug)]
pub struct Filter {
    pub r#type: String,
    pub tagk: String,
    pub filter: String,
    #[serde(rename = "groupBy")]
    pub group_by: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubQuery {
    pub metric: String,
    pub aggregator: String,
    #[serde(default)]
    pub rate: bool,
    pub downsample: Option<String>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub filters: Vec<Filter>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRequest {
    pub start: i64,
    pub end: i64,
    pub queries: Vec<SubQuery>,
    #[serde(rename = "msResolution", default)]
    pub ms_resolution: bool,
}

pub struct OpentsdbSubPlan {
    pub plan: Plan,
    pub field_col_name: String,
    pub timestamp_col_name: String,
}

pub struct OpentsdbQueryPlan {
    pub plans: Vec<OpentsdbSubPlan>,
}
