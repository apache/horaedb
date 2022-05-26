// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
