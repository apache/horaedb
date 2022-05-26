// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Bytes that can safely cast to str/string.

use std::{convert::TryFrom, fmt, ops, str};

use snafu::{Backtrace, ResultExt, Snafu};

use crate::bytes::Bytes;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Bytes are not valid utf8, err:{}.\nBacktrace:\n{}", source, backtrace))]
    FromBytes {
        source: std::str::Utf8Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// String using [crate::bytes::Bytes] as storage so it can be cast into `Bytes`
/// and clone like `Bytes`.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct StringBytes(Bytes);

impl StringBytes {
    pub fn new() -> StringBytes {
        StringBytes(Bytes::new())
    }

    pub const fn from_static(src: &'static str) -> StringBytes {
        StringBytes(Bytes::from_static(src.as_bytes()))
    }

    pub fn copy_from_str(src: &str) -> StringBytes {
        StringBytes(Bytes::copy_from_slice(src.as_bytes()))
    }

    /// Create a [StringBytes] from a valid utf bytes.
    ///
    /// # Safety
    /// The caller must ensure `bytes` is valid utf string.
    pub unsafe fn from_bytes_unchecked(bytes: Bytes) -> StringBytes {
        StringBytes(bytes)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.as_bytes()) }
    }
}

impl Default for StringBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl ops::Deref for StringBytes {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for StringBytes {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for StringBytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<Bytes> for StringBytes {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<StringBytes> {
        str::from_utf8(&bytes).context(FromBytes)?;

        Ok(StringBytes(bytes))
    }
}

impl From<String> for StringBytes {
    fn from(src: String) -> Self {
        Self(Bytes::from(src))
    }
}

impl From<&str> for StringBytes {
    fn from(src: &str) -> Self {
        Self::copy_from_str(src)
    }
}
