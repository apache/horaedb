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

use crate::bits::{Bit, Error};

const BIT_MASKS: [u8; 8] = [128, 64, 32, 16, 8, 4, 2, 1];

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

    fn get_byte(&mut self) -> Result<u8, Error> {
        self.bytes.get(self.byte_idx).cloned().ok_or(Error::Eof)
    }
}

impl<'a> BufferedReader<'a> {
    pub fn next_bit(&mut self) -> Result<Bit, Error> {
        if self.bit_idx == 8 {
            self.byte_idx += 1;
            self.bit_idx = 0;
        }

        let byte = self.get_byte()?;

        let bit = if byte & BIT_MASKS[self.bit_idx as usize] == 0 {
            Bit::Zero
        } else {
            Bit::One
        };

        self.bit_idx += 1;

        Ok(bit)
    }

    pub fn next_byte(&mut self) -> Result<u8, Error> {
        if self.bit_idx == 0 {
            self.bit_idx += 8;
            return self.get_byte();
        }

        if self.bit_idx == 8 {
            self.byte_idx += 1;
            return self.get_byte();
        }

        let mut byte = 0;
        let mut b = self.get_byte()?;

        byte |= b.wrapping_shl(self.bit_idx);

        self.byte_idx += 1;
        b = self.get_byte()?;

        byte |= b.wrapping_shr(8 - self.bit_idx);

        Ok(byte)
    }

    /// Fetch the next `num` bits, and advance the inner cursor. And the
    /// returned bits are stored in a u64 integer in little-endian order.
    ///
    /// Example: the returned value will be `0x0000 0000 0000 000F` if 4 bits is
    /// fetched and they are all set.
    pub fn next_bits(&mut self, mut num: u32) -> Result<u64, Error> {
        // can't read more than 64 bits into a u64
        assert!(num <= 64);

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.next_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.next_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;

            num -= 1;
        }

        Ok(bits)
    }

    #[allow(dead_code)]
    pub fn peak_bits(&mut self, num: u32) -> Result<u64, Error> {
        // save the current index and pos so we can reset them after calling `read_bits`
        let index = self.byte_idx;
        let pos = self.bit_idx;

        let bits = self.next_bits(num)?;

        self.byte_idx = index;
        self.bit_idx = pos;

        Ok(bits)
    }
}

#[cfg(test)]
mod tests {
    use crate::bits::{Bit, BufferedReader, Error};

    #[test]
    fn next_bit() {
        let bytes = vec![0b01101100, 0b11101001];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);

        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bit().unwrap(), Bit::One);

        assert_eq!(b.next_bit().err().unwrap(), Error::Eof);
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
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);
        assert_eq!(b.next_bit().unwrap(), Bit::One);

        assert_eq!(b.next_byte().unwrap(), 15);

        assert_eq!(b.next_byte().err().unwrap(), Error::Eof);
    }

    #[test]
    fn next_bits() {
        let bytes = vec![0b01010111, 0b00011101, 0b11110101, 0b00010100];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bits(3).unwrap(), 0b010);
        assert_eq!(b.next_bits(1).unwrap(), 0b1);
        assert_eq!(b.next_bits(20).unwrap(), 0b01110001110111110101);
        assert_eq!(b.next_bits(8).unwrap(), 0b00010100);
        assert_eq!(b.next_bits(4).err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_mixed() {
        let bytes = vec![0b01101101, 0b01101101];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bits(3).unwrap(), 0b110);
        assert_eq!(b.next_byte().unwrap(), 0b11010110);
        assert_eq!(b.next_bits(2).unwrap(), 0b11);
        assert_eq!(b.next_bit().unwrap(), Bit::Zero);
        assert_eq!(b.next_bits(1).unwrap(), 0b1);
        assert_eq!(b.next_bit().err().unwrap(), Error::Eof);
    }

    #[test]
    fn peak_bits() {
        let bytes = vec![0b01010111, 0b00011101, 0b11110101, 0b00010100];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.peak_bits(1).unwrap(), 0b0);
        assert_eq!(b.peak_bits(4).unwrap(), 0b0101);
        assert_eq!(b.peak_bits(8).unwrap(), 0b01010111);
        assert_eq!(b.peak_bits(20).unwrap(), 0b01010111000111011111);

        // read some individual bits we can test `peak_bits` when the position in the
        // byte we are currently reading is non-zero
        assert_eq!(b.next_bits(12).unwrap(), 0b010101110001);

        assert_eq!(b.peak_bits(1).unwrap(), 0b1);
        assert_eq!(b.peak_bits(4).unwrap(), 0b1101);
        assert_eq!(b.peak_bits(8).unwrap(), 0b11011111);
        assert_eq!(b.peak_bits(20).unwrap(), 0b11011111010100010100);

        assert_eq!(b.peak_bits(22).err().unwrap(), Error::Eof);
    }
}
