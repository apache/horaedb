use std::{error, fmt};

/// Bit
///
/// An enum used to represent a single bit, can be either `Zero` or `One`.
#[derive(Debug, PartialEq)]
pub enum Bit {
    Zero,
    One,
}

impl Bit {
    /// Convert a bit to u64, so `Zero` becomes 0 and `One` becomes 1.
    pub fn to_u64(&self) -> u64 {
        match self {
            Bit::Zero => 0,
            Bit::One => 1,
        }
    }
}

/// Error
///
/// Enum used to represent potential errors when interacting with a stream.
#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Eof => write!(f, "Encountered the end of the stream"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Eof => "Encountered the end of the stream",
        }
    }
}

/// Read
///
/// Read is a trait that encapsulates the functionality required to read from a
/// stream of bytes.
pub trait Read {
    /// Read a single bit from the underlying stream.
    fn read_bit(&mut self) -> Result<Bit, Error>;

    /// Read a single byte from the underlying stream.
    fn read_byte(&mut self) -> Result<u8, Error>;

    /// Read `num` bits from the underlying stream.
    fn read_bits(&mut self, num: u32) -> Result<u64, Error>;

    /// Get the next `num` bits, but do not update place in stream.
    fn peak_bits(&mut self, num: u32) -> Result<u64, Error>;
}

/// Write
///
/// Write is a trait that encapsulates the functionality required to write a
/// stream of bytes.
pub trait Write {
    // Write a single bit to the underlying stream.
    fn write_bit(&mut self, bit: Bit);

    // Write a single byte to the underlying stream.
    fn write_byte(&mut self, byte: u8);

    // Write the bottom `num` bits of `bits` to the underlying stream.
    fn write_bits(&mut self, bits: u64, num: u32);

    // Close the underlying stream and return a pointer to the array of bytes.
    fn close(self) -> Box<[u8]>;

    fn len(&self) -> usize;
}

pub mod buffered_write;

pub use self::buffered_write::BufferedWriter;

pub mod buffered_read;

pub use self::buffered_read::BufferedReader;
