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

        #[snafu(display(
            "Rewrite influxql from clause with cause, msg:{}, source:{}",
            msg,
            source
        ))]
        RewriteFromWithCause { msg: String, source: GenericError },

        #[snafu(display("Rewrite influxql from clause no cause, msg:{}", msg))]
        RewriteFromNoCause { msg: String },

        #[snafu(display("Unimplemented influxql select fields with cause, source: {}", source))]
        RewriteFieldsWithCause { msg: String, source: GenericError },

        #[snafu(display("Unimplemented influxql select fields no cause, msg: {}", msg))]
        RewriteFieldsNoCause { msg: String },
    }

    define_result!(Error);
}
