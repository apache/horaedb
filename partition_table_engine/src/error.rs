// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use common_util::{define_result, error::GenericError};
use snafu::Snafu;
define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error, message:{}, err:{}", msg, source))]
    Internal { msg: String, source: GenericError },
}
