use macros::define_result;
use snafu::Snafu;

use self::types::{OpentsdbQueryPlan, QueryRequest};
use crate::provider::{ContextProviderAdapter, MetaProvider};

pub mod types;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub fn opentsdb_query_to_plan<P: MetaProvider>(
    query: QueryRequest,
    meta_provider: ContextProviderAdapter<'_, P>,
) -> Result<OpentsdbQueryPlan> {
    todo!()
}
