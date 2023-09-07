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

use crate::bits::{Bit, BIT_MASKS};

/// BufferedReader
/// BufferedReader encapsulates a buffer of bytes which can be read from.
#[derive(Debug)]
pub struct BufferedReader<'a> {
    /// internal buffer of bytes
    bytes: &'a [u8],
    /// index into bytes
    byte_idx: usize,
    /// position in the byte we are currently reading
    bit_idx: u32,
}

impl<'a> BufferedReader<'a> {
    /// new creates a new `BufferedReader` from `bytes`
    pub fn new(bytes: &'a [u8]) -> Self {
        BufferedReader {
            bytes,
            byte_idx: 0,
            bit_idx: 0,
        }
    }

    fn get_byte(&self) -> Option<u8> {
        if self.is_eof() {
            return None;
        }
        self.bytes.get(self.byte_idx).cloned()
    }

    #[inline]
    fn is_eof(&self) -> bool {
        self.byte_idx >= self.bytes.len()
    }

    fn advance_one_bit(&mut self) {
        if self.bit_idx == 7 {
            self.bit_idx = 0;
            self.byte_idx += 1;
        } else {
            self.bit_idx += 1;
        }
    }
}

impl<'a> BufferedReader<'a> {
    pub fn next_bit(&mut self) -> Option<Bit> {
        if self.is_eof() {
            return None;
        }
        let byte = self.get_byte().unwrap();
        let bit = Bit(u8::from(byte & BIT_MASKS[self.bit_idx as usize] != 0));
        self.advance_one_bit();
        Some(bit)
    }

    fn next_byte(&mut self) -> Option<u8> {
        if self.bit_idx == 0 {
            let byte = self.get_byte();
            self.byte_idx += 1;
            return byte;
        }

        let mut byte = 0;
        let mut b = match self.get_byte() {
            None => return None,
            Some(b) => b,
        };

        byte |= b << self.bit_idx;

        self.byte_idx += 1;
        b = match self.get_byte() {
            None => return None,
            Some(b) => b,
        };

        byte |= b >> (8 - self.bit_idx);

        Some(byte)
    }

    /// Fetch the next `num` bits, and advance the inner cursor. And the
    /// returned bits are stored in a u64 integer in little-endian order.
    ///
    /// Example: the returned value will be `0x0000 0000 0000 000F` if 4 bits is
    /// fetched and they are all set.
    pub fn next_bits(&mut self, mut num: u32) -> Option<u64> {
        // can't read more than 64 bits into a u64
        assert!(num <= 64);

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.next_byte().map(u64::from)?;
            bits = bits << 8 | byte;
            num -= 8;
        }

        while num > 0 {
            self.next_bit().map(|bit| bits = bits << 1 | bit.0 as u64)?;

            num -= 1;
        }

        Some(bits)
    }
}

#[cfg(test)]
mod tests {
    use crate::bits::{Bit, BufferedReader};

    #[test]
    fn next_bit() {
        let bytes = vec![0b01101100, 0b11101001];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(0));

        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bit().unwrap(), Bit(1));

        assert_eq!(b.next_bit(), None);
    }

    #[test]
    fn next_byte() {
        let bytes = vec![100, 25, 0, 240, 240];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_byte().unwrap(), 100);
        assert_eq!(b.next_byte().unwrap(), 25);
        assert_eq!(b.next_byte().unwrap(), 0);

        // read some individual bits we can test `read_byte` when the position in the
        // byte we are currently reading is non-zero
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));
        assert_eq!(b.next_bit().unwrap(), Bit(1));

        assert_eq!(b.next_byte().unwrap(), 15);

        assert_eq!(b.next_byte(), None);
    }

    #[test]
    fn next_bits() {
        let bytes = vec![0b01010111, 0b00011101, 0b11110101, 0b00010100];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bits(3).unwrap(), 0b010);
        assert_eq!(b.next_bits(1).unwrap(), 0b1);
        assert_eq!(b.next_bits(20).unwrap(), 0b01110001110111110101);
        assert_eq!(b.next_bits(8).unwrap(), 0b00010100);
        assert_eq!(b.next_bits(4), None);
    }

    #[test]
    fn read_mixed() {
        let bytes = vec![0b01101101, 0b01101101];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bits(3).unwrap(), 0b110);
        assert_eq!(b.next_byte().unwrap(), 0b11010110);
        assert_eq!(b.next_bits(2).unwrap(), 0b11);
        assert_eq!(b.next_bit().unwrap(), Bit(0));
        assert_eq!(b.next_bits(1).unwrap(), 0b1);
        assert_eq!(b.next_bit(), None);
    }
}
