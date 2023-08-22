use std::boxed::Box;

use crate::bits::{Bit, Write};

/// BufferedWriter
///
/// BufferedWriter writes bytes to a buffer.
#[derive(Debug, Default, Clone)]
pub struct BufferedWriter {
    buf: Vec<u8>,
    pos: u32, // position in the last byte in the buffer
}

impl BufferedWriter {
    /// new creates a new BufferedWriter
    pub fn new() -> Self {
        BufferedWriter {
            buf: Vec::new(),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        BufferedWriter {
            buf: Vec::with_capacity(capacity),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    pub fn with_buf(buf: Vec<u8>) -> Self {
        BufferedWriter {
            buf,
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    fn grow(&mut self) {
        self.buf.push(0);
    }

    fn last_index(&self) -> usize {
        self.buf.len() - 1
    }
}

impl Write for BufferedWriter {
    fn write_bit(&mut self, bit: Bit) {
        if self.pos == 8 {
            self.grow();
            self.pos = 0;
        }

        let i = self.last_index();

        match bit {
            Bit::Zero => (),
            Bit::One => self.buf[i] |= 1u8.wrapping_shl(7 - self.pos),
        };

        self.pos += 1;
    }

    fn write_byte(&mut self, byte: u8) {
        if self.pos == 8 {
            self.grow();

            let i = self.last_index();
            self.buf[i] = byte;
            return;
        }

        let i = self.last_index();
        let mut b = byte.wrapping_shr(self.pos);
        self.buf[i] |= b;

        self.grow();

        b = byte.wrapping_shl(8 - self.pos);
        self.buf[i + 1] |= b;
    }

    // example: wtire_bits(4): data(u64 0000 0000 0000 00ff), write data 1111
    fn write_bits(&mut self, mut bits: u64, mut num: u32) {
        // we should never write more than 64 bits for a u64
        if num > 64 {
            num = 64;
        }

        bits = bits.wrapping_shl(64 - num);
        while num >= 8 {
            let byte = bits.wrapping_shr(56);
            self.write_byte(byte as u8);

            bits = bits.wrapping_shl(8);
            num -= 8;
        }

        while num > 0 {
            let byte = bits.wrapping_shr(63);
            if byte == 1 {
                self.write_bit(Bit::One);
            } else {
                self.write_bit(Bit::Zero);
            }

            bits = bits.wrapping_shl(1);
            num -= 1;
        }
    }

    fn close(self) -> Box<[u8]> {
        self.buf.into_boxed_slice()
    }

    fn len(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedWriter;
    use crate::bits::{Bit, Write};

    #[test]
    fn write_bit() {
        let mut b = BufferedWriter::new();

        // 170 = 0b10101010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(Bit::One);
                continue;
            }

            b.write_bit(Bit::Zero);
        }

        // 146 = 0b10010010
        for i in 0..8 {
            if i % 3 == 0 {
                b.write_bit(Bit::One);
                continue;
            }

            b.write_bit(Bit::Zero);
        }

        // 136 = 010001000
        for i in 0..8 {
            if i % 4 == 0 {
                b.write_bit(Bit::One);
                continue;
            }

            b.write_bit(Bit::Zero);
        }

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 170);
        assert_eq!(b.buf[1], 146);
        assert_eq!(b.buf[2], 136);
    }

    #[test]
    fn write_byte() {
        let mut b = BufferedWriter::new();

        b.write_byte(234);
        b.write_byte(188);
        b.write_byte(77);

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 234);
        assert_eq!(b.buf[1], 188);
        assert_eq!(b.buf[2], 77);

        // write some bits so we can test `write_byte` when the last byte is partially
        // filled
        b.write_bit(Bit::One);
        b.write_bit(Bit::One);
        b.write_bit(Bit::One);
        b.write_bit(Bit::One);
        b.write_byte(0b11110000); // 1111 1111 0000
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111 0000 1111

        assert_eq!(b.buf.len(), 7);
        assert_eq!(b.buf[3], 255); // 0b11111111 = 255
        assert_eq!(b.buf[4], 0); // 0b00000000 = 0
        assert_eq!(b.buf[5], 240); // 0b11110000 = 240
    }

    #[test]
    fn write_bits() {
        let mut b = BufferedWriter::new();

        // 101011
        b.write_bits(43, 6);

        // 010
        b.write_bits(2, 3);

        // 1
        b.write_bits(1, 1);

        // 1010 1100 1110 0011 1101
        b.write_bits(708157, 20);

        // 11
        b.write_bits(3, 2);

        assert_eq!(b.buf.len(), 4);

        assert_eq!(b.buf[0], 173); // 0b10101101 = 173
        assert_eq!(b.buf[1], 107); // 0b01101011 = 107
        assert_eq!(b.buf[2], 56); // 0b00111000 = 56
        assert_eq!(b.buf[3], 247); // 0b11110111 = 247
    }

    #[test]
    fn write_mixed() {
        let mut b = BufferedWriter::new();

        // 1010 1010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(Bit::One);
                continue;
            }

            b.write_bit(Bit::Zero);
        }

        // 0000 1001
        b.write_byte(9);

        // 1001 1100 1100
        b.write_bits(2508, 12);

        println!("{:?}", b.buf);

        // 1111
        for _ in 0..4 {
            b.write_bit(Bit::One);
        }

        assert_eq!(b.buf.len(), 4);

        println!("{:?}", b.buf);

        assert_eq!(b.buf[0], 170); // 0b10101010 = 170
        assert_eq!(b.buf[1], 9); // 0b00001001 = 9
        assert_eq!(b.buf[2], 156); // 0b10011100 = 156
        assert_eq!(b.buf[3], 207); // 0b11001111 = 207
    }
}
