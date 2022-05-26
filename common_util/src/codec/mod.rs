// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Data encoding

// TODO(yingwen): Buf use generic type to avoid cost of vtable call per
// encode/decode

pub mod compact;
mod consts;
pub mod memcomparable;
pub mod row;
mod varint;

use common_types::bytes::{MemBuf, MemBufMut};

// encoder/decoder
/// Data encode abstraction
pub trait Encoder<T: ?Sized> {
    type Error;

    /// Encode value into buf
    fn encode<B: MemBufMut>(&self, buf: &mut B, value: &T) -> Result<(), Self::Error>;

    /// Estimate the value size after encoded
    fn estimate_encoded_size(&self, value: &T) -> usize;
}

/// Data decode to target
pub trait DecodeTo<T> {
    type Error;

    /// Decode from `buf` to `value`
    fn decode_to<B: MemBuf>(&self, buf: &mut B, value: &mut T) -> Result<(), Self::Error>;
}

/// Data decode abstraction
pub trait Decoder<T> {
    type Error;

    /// Decode `value` from `buf`
    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<T, Self::Error>;
}
