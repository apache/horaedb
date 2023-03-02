pub mod planner;
mod stmt_rewriter;
pub(crate) mod util;
pub mod error {
    use common_util::error::GenericError;
    use snafu::Snafu;

    #[derive(Debug, Snafu)]
    #[snafu(visibility = "pub")]
    pub enum Error {
        #[snafu(display("Unimplemented influxql statement, msg: {}", msg))]
        Unimplemented { msg: String },

        #[snafu(display("Unimplemented influxql statement, source: {}", source))]
        RewriteStmtWithCause { source: GenericError },
    }

    define_result!(Error);
}
