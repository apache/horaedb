use crate::bits::{Bit, Error, Read};

/// BufferedReader
///
/// BufferedReader encapsulates a buffer of bytes which can be read from.
#[derive(Debug)]
pub struct BufferedReader<'a> {
    bytes: &'a [u8], // internal buffer of bytes
    index: usize,    // index into bytes
    pos: u32,        // position in the byte we are currently reading
}

impl<'a> BufferedReader<'a> {
    /// new creates a new `BufferedReader` from `bytes`
    pub fn new(bytes: &'a [u8]) -> Self {
        BufferedReader {
            bytes,
            index: 0,
            pos: 0,
        }
    }

    fn get_byte(&mut self) -> Result<u8, Error> {
        self.bytes.get(self.index).cloned().ok_or(Error::Eof)
    }
}

impl<'a> Read for BufferedReader<'a> {
    fn read_bit(&mut self) -> Result<Bit, Error> {
        if self.pos == 8 {
            self.index += 1;
            self.pos = 0;
        }

        let byte = self.get_byte()?;

        let bit = if byte & 1u8.wrapping_shl(7 - self.pos) == 0 {
            Bit::Zero
        } else {
            Bit::One
        };

        self.pos += 1;

        Ok(bit)
    }

    fn read_byte(&mut self) -> Result<u8, Error> {
        if self.pos == 0 {
            self.pos += 8;
            return self.get_byte();
        }

        if self.pos == 8 {
            self.index += 1;
            return self.get_byte();
        }

        let mut byte = 0;
        let mut b = self.get_byte()?;

        byte |= b.wrapping_shl(self.pos);

        self.index += 1;
        b = self.get_byte()?;

        byte |= b.wrapping_shr(8 - self.pos);

        Ok(byte)
    }

    // example: read_bits(4): read data:u64 0000 0000 0000 000f
    // datastore in low position
    fn read_bits(&mut self, mut num: u32) -> Result<u64, Error> {
        // can't read more than 64 bits into a u64
        if num > 64 {
            num = 64;
        }

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.read_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.read_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;

            num -= 1;
        }

        Ok(bits)
    }

    fn peak_bits(&mut self, num: u32) -> Result<u64, Error> {
        // save the current index and pos so we can reset them after calling `read_bits`
        let index = self.index;
        let pos = self.pos;

        let bits = self.read_bits(num)?;

        self.index = index;
        self.pos = pos;

        Ok(bits)
    }
}

#[cfg(test)]
mod tests {
    use crate::bits::{Bit, BufferedReader, Error, Read};

    #[test]
    fn read_bit() {
        let bytes = vec![0b01101100, 0b11101001];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);

        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bit().unwrap(), Bit::One);

        assert_eq!(b.read_bit().err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_byte() {
        let bytes = vec![100, 25, 0, 240, 240];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_byte().unwrap(), 100);
        assert_eq!(b.read_byte().unwrap(), 25);
        assert_eq!(b.read_byte().unwrap(), 0);

        // read some individual bits we can test `read_byte` when the position in the
        // byte we are currently reading is non-zero
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);
        assert_eq!(b.read_bit().unwrap(), Bit::One);

        assert_eq!(b.read_byte().unwrap(), 15);

        assert_eq!(b.read_byte().err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_bits() {
        let bytes = vec![0b01010111, 0b00011101, 0b11110101, 0b00010100];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_bits(3).unwrap(), 0b010);
        assert_eq!(b.read_bits(1).unwrap(), 0b1);
        assert_eq!(b.read_bits(20).unwrap(), 0b01110001110111110101);
        assert_eq!(b.read_bits(8).unwrap(), 0b00010100);
        assert_eq!(b.read_bits(4).err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_mixed() {
        let bytes = vec![0b01101101, 0b01101101];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bits(3).unwrap(), 0b110);
        assert_eq!(b.read_byte().unwrap(), 0b11010110);
        assert_eq!(b.read_bits(2).unwrap(), 0b11);
        assert_eq!(b.read_bit().unwrap(), Bit::Zero);
        assert_eq!(b.read_bits(1).unwrap(), 0b1);
        assert_eq!(b.read_bit().err().unwrap(), Error::Eof);
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
        assert_eq!(b.read_bits(12).unwrap(), 0b010101110001);

        assert_eq!(b.peak_bits(1).unwrap(), 0b1);
        assert_eq!(b.peak_bits(4).unwrap(), 0b1101);
        assert_eq!(b.peak_bits(8).unwrap(), 0b11011111);
        assert_eq!(b.peak_bits(20).unwrap(), 0b11011111010100010100);

        assert_eq!(b.peak_bits(22).err().unwrap(), Error::Eof);
    }
}
