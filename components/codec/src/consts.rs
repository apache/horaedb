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

//! Common constants used in codec

// First byte in the encoded value which specifies the encoding type.
// TODO(yingwen): Replace flags by datum kind. (Incompatible with old format).
pub const NULL_FLAG: u8 = 0;
pub const BYTES_FLAG: u8 = 1;
pub const COMPACT_BYTES_FLAG: u8 = 2;
pub const INT_FLAG: u8 = 3;
pub const UINT_FLAG: u8 = 4;
pub const FLOAT_FLAG: u8 = 5;
pub const VARINT_FLAG: u8 = 8;
pub const UVARINT_FLAG: u8 = 9;

/// Max bytes varint can use
pub const MAX_VARINT_BYTES: usize = 10;
/// Max bytes uvarint can be use
pub const MAX_UVARINT_BYTES: usize = 10;
/// Sign mask for u64/i64 conversion
pub const SIGN_MASK: u64 = 0x8000000000000000;
