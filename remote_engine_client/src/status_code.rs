// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Remote service status code

/// A set of codes for meta event service.
///
/// Note that such a set of codes is different with the codes (alias to http
/// status code) used by storage service.
#[allow(dead_code)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StatusCode {
    #[default]
    Ok = 0,
    BadRequest = 401,
    NotFound = 404,
    Internal = 500,
}

impl StatusCode {
    #[inline]
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

#[inline]
pub fn is_ok(code: u32) -> bool {
    code == StatusCode::Ok.as_u32()
}
