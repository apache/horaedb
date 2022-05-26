// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

// Copyright 2020 Datafuse Labs.
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

// fork from:https://github.com/datafuselabs/databend/tree/master/common/tracing

mod logging;

pub use logging::{init_default_tracing, init_default_ut_tracing, init_tracing_with_file};
pub use tracing_appender;
