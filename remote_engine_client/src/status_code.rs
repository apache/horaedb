// Copyright 2023 The CeresDB Authors
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
