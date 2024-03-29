// Copyright 2020 Datafuse Labs.
// fork from:https://github.com/datafuselabs/databend/tree/master/common/tracing

mod logging;

pub use logging::{init_default_tracing, init_default_ut_tracing, init_tracing_with_file, Config};
pub use tracing_appender;
