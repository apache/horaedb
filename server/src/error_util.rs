// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

/// Returns first line in error message, now we use this hack to exclude
/// backtrace from error message that returned to user.
// TODO: Consider a better way to get the error message.
pub fn first_line_in_error(err_string: &str) -> &str {
    err_string.split('\n').next().unwrap_or(err_string)
}
