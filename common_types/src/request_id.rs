// Copyright 2023 The HoraeDB Authors
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

//! Request id.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(String);

impl RequestId {
    /// Acquire next request id.
    pub fn next_id() -> Self {
        let id = "".to_string();
        Self(id)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_string(self) -> String {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for RequestId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for RequestId {
    fn from(id: &str) -> Self {
        Self(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id() {
        let id = RequestId::next_id();
        let id2 = RequestId::next_id();

        assert_ne!(id, id2);
    }
}
