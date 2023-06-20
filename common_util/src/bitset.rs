// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! BitSet supports counting set/unset bits.

#[derive(Debug, Default, Clone)]
pub struct BitSet {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: Vec<u8>,
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl BitSet {
    /// Initialize a unset [`BitSet`].
    pub fn new(num_bits: usize) -> Self {
        let len = (num_bits + 7) >> 3;
        Self {
            buffer: vec![0; len],
            num_bits,
        }
    }

    /// Initialize directly from a buffer.
    pub fn try_from_raw(buffer: Vec<u8>, num_bits: usize) -> Option<Self> {
        if buffer.len() < num_bits >> 3 {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        self.buffer[byte_index] |= 1 << bit_index;
        true
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        if index >= self.num_bits {
            return None;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        let set = (self.buffer[byte_index] & (1 << bit_index)) != 0;
        Some(set)
    }

    /// Compute the number of the set bits before the bit at `index`.
    pub fn num_unset_bits_before(&self, index: usize) -> Option<usize> {
        self.num_set_bits_before(index).map(|num_set_bits| {
            let num_bits = 1 + index;
            num_bits - num_set_bits
        })
    }

    /// Compute the number of the set bits before the bit at `index`.
    pub fn num_set_bits_before(&self, index: usize) -> Option<usize> {
        if index >= self.num_bits {
            return None;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);

        let mut num_set_bits = 0;
        for b in &self.buffer[0..byte_index] {
            num_set_bits += count_set_bits(*b) as u32;
        }

        num_set_bits += count_set_bits_before(self.buffer[byte_index], bit_index) as u32;

        Some(num_set_bits as usize)
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    #[inline]
    fn compute_byte_bit_index(index: usize) -> (usize, usize) {
        (index >> 3, index & 7)
    }
}

fn count_set_bits(b: u8) -> u8 {
    static LOOKUP: [u8; 16] = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4];

    LOOKUP[(b & 0x0F) as usize] + LOOKUP[(b >> 4) as usize]
}

fn count_set_bits_before(b: u8, idx: usize) -> u8 {
    let mut num_bits = 0;
    for i in 0..idx {
        num_bits += b >> i & 1;
    }

    num_bits
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::BitSet;

    #[test]
    fn test_set_op() {
        let mut bit_set = BitSet::new(50);

        assert!(bit_set.set(1));
        assert!(bit_set.is_set(1).unwrap());

        assert!(bit_set.set(20));
        assert!(bit_set.is_set(20).unwrap());
        assert!(bit_set.set(49));
        assert!(bit_set.is_set(49).unwrap());

        assert!(!bit_set.set(100));
        assert!(bit_set.is_set(100).is_none());

        assert_eq!(
            bit_set.into_bytes(),
            vec![
                0b00000010,
                0b00000000,
                0b00010000,
                0b000000000,
                0b00000000,
                0b00000000,
                0b00000010
            ]
        );
    }

    #[test]
    fn test_num_bits_before() {
        let raw_bytes: Vec<u8> = vec![0b11111111, 0b11110000, 0b00001111, 0b00001100, 0b00001001];
        let bit_set = BitSet::try_from_raw(raw_bytes, 35).unwrap();
        assert_eq!(bit_set.num_set_bits_before(1), Some(1));
        assert_eq!(bit_set.num_set_bits_before(9), Some(8));
        assert_eq!(bit_set.num_set_bits_before(13), Some(9));
        assert_eq!(bit_set.num_set_bits_before(30), Some(18));
        assert_eq!(bit_set.num_set_bits_before(34), Some(19));
        assert_eq!(bit_set.num_set_bits_before(49), None);

        assert_eq!(bit_set.is_set(7), Some(true));
        assert_eq!(bit_set.is_set(8), Some(false));
        assert_eq!(bit_set.is_set(34), Some(false));
        assert_eq!(bit_set.is_set(35), None);
        assert_eq!(bit_set.is_set(36), None);
    }

    #[test]
    fn test_try_from_raw() {
        let raw_bytes: Vec<u8> = vec![0b11111111, 0b11110000, 0b00001111, 0b00001100, 0b00001001];
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 50).is_none());
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 40).is_some());
        assert!(BitSet::try_from_raw(raw_bytes, 1).is_some());
    }
}
