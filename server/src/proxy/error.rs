use common_util::error::GenericError;
use snafu::Snafu;

define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("internal error, msg:{}, cause:{}", msg, source))]
    Internal { msg: String, source: GenericError },
}
