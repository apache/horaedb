// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use generic_error::GenericError;
use macros::define_result;
use snafu::Snafu;
define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error, message:{}, err:{}", msg, source))]
    Internal { msg: String, source: GenericError },
}
