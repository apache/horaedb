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

//! Data encoding

// TODO(yingwen): Buf use generic type to avoid cost of vtable call per
// encode/decode

mod bits;
pub mod columnar;
pub mod compact;
mod consts;
pub mod memcomparable;
pub mod row;
mod varint;

use bytes_ext::{Buf, BufMut};

// encoder/decoder
/// Data encode abstraction
pub trait Encoder<T: ?Sized> {
    type Error;

    /// Encode value into buf
    fn encode<B: BufMut>(&self, buf: &mut B, value: &T) -> Result<(), Self::Error>;

    /// Estimate the value size after encoded
    fn estimate_encoded_size(&self, value: &T) -> usize;
}

/// Data decode to target
pub trait DecodeTo<T> {
    type Error;

    /// Decode from `buf` to `value`
    fn decode_to<B: Buf>(&self, buf: &mut B, value: &mut T) -> Result<(), Self::Error>;
}

/// Data decode abstraction
pub trait Decoder<T> {
    type Error;

    /// Decode `value` from `buf`
    fn decode<B: Buf>(&self, buf: &mut B) -> Result<T, Self::Error>;
}
