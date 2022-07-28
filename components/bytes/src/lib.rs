// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Provides utilities for byte arrays
//!
//! Use Bytes instead of Vec<u8>. Currently just re-export bytes crate

use std::{
    fmt,
    io::{self, Read, Write},
};

use snafu::{ensure, Backtrace, GenerateBacktrace, Snafu};
// Should not use bytes crate outside of this mod so we can replace the actual
// implementations if needed
pub use upstream::{Buf, BufMut, Bytes, BytesMut};

/// Error of MemBuf/MemBufMut
///
/// We do not use `std::io::Error` because it is too large
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to fill whole buffer.\nBacktrace:\n{}", backtrace))]
    UnexpectedEof { backtrace: Backtrace },

    #[snafu(display("Failed to write whole buffer.\nBacktrace:\n{}", backtrace))]
    WouldOverflow { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Now is just an alias to `Vec<u8>`, prefer to use this alias instead of
/// `Vec<u8>`
pub type ByteVec = Vec<u8>;

/// Read bytes from a buffer.
///
/// Unlike `bytes::Buf`, the underlying storage is in contiguous memory
pub trait MemBuf: fmt::Debug {
    /// Return the remaining byte slice
    fn remaining_slice(&self) -> &[u8];

    /// Advance the internal cursor of the buffer, panic if overflow
    fn must_advance(&mut self, cnt: usize);

    /// Read bytes from self into dst.
    ///
    /// The cursor is advanced by the number of bytes copied.
    ///
    /// Returns error if self does not have enough remaining bytes to fill dst.
    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()>;

    /// Gets an unsigned 8 bit integer from self and advance current position
    ///
    /// Returns error if the capacity is not enough
    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_to_slice(&mut buf)?;
        Ok(buf[0])
    }

    /// Gets an unsighed 32 bit integer from self in big-endian byte order and
    /// advance current position
    ///
    /// Returns error if the capacity is not enough
    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.read_to_slice(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    /// Gets an unsighed 64 bit integer from self in big-endian byte order and
    /// advance current position
    ///
    /// Returns error if the capacity is not enough
    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.read_to_slice(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let mut buf = [0; 8];
        self.read_to_slice(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let mut buf = [0; 4];
        self.read_to_slice(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }
}

/// Write bytes to a buffer
///
/// Unlike `bytes::BufMut`, write operations may fail
pub trait MemBufMut: fmt::Debug {
    /// Write bytes into self from src, advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_slice(&mut self, src: &[u8]) -> Result<()>;

    /// Write an unsigned 8 bit integer to self, advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_u8(&mut self, n: u8) -> Result<()> {
        let src = [n];
        self.write_slice(&src)
    }

    /// Writes an unsigned 32 bit integer to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_u32(&mut self, n: u32) -> Result<()> {
        self.write_slice(&n.to_be_bytes())
    }

    /// Writes an unsigned 64 bit integer to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_u64(&mut self, n: u64) -> Result<()> {
        self.write_slice(&n.to_be_bytes())
    }

    /// Writes an float 64 to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_f64(&mut self, n: f64) -> Result<()> {
        self.write_slice(&n.to_be_bytes())
    }

    /// Writes an float 32 to self in the big-endian byte order,
    /// advance the buffer position
    ///
    /// Returns error if the capacity is not enough
    fn write_f32(&mut self, n: f32) -> Result<()> {
        self.write_slice(&n.to_be_bytes())
    }
}

macro_rules! impl_mem_buf {
    () => {
        #[inline]
        fn remaining_slice(&self) -> &[u8] {
            &self
        }

        #[inline]
        fn must_advance(&mut self, cnt: usize) {
            self.advance(cnt);
        }

        #[inline]
        fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
            ensure!(self.remaining() >= dst.len(), UnexpectedEof);
            self.copy_to_slice(dst);
            Ok(())
        }
    };
}

impl MemBuf for Bytes {
    impl_mem_buf!();
}

impl MemBuf for BytesMut {
    impl_mem_buf!();
}

impl MemBufMut for BytesMut {
    fn write_slice(&mut self, src: &[u8]) -> Result<()> {
        ensure!(self.remaining_mut() >= src.len(), WouldOverflow);
        self.put_slice(src);
        Ok(())
    }
}

impl MemBuf for &[u8] {
    #[inline]
    fn remaining_slice(&self) -> &[u8] {
        self
    }

    #[inline]
    fn must_advance(&mut self, cnt: usize) {
        *self = &self[cnt..];
    }

    #[inline]
    fn read_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
        // slice::read_exact() only throws UnexpectedEof error, see
        //
        // https://doc.rust-lang.org/src/std/io/impls.rs.html#264-281
        self.read_exact(dst).map_err(|_| Error::UnexpectedEof {
            backtrace: Backtrace::generate(),
        })
    }
}

impl MemBufMut for &mut [u8] {
    fn write_slice(&mut self, src: &[u8]) -> Result<()> {
        // slice::write_all() actually wont fail, see
        //
        // https://doc.rust-lang.org/src/std/io/impls.rs.html#344-350
        self.write_all(src).map_err(|_| Error::WouldOverflow {
            backtrace: Backtrace::generate(),
        })
    }
}

impl MemBufMut for Vec<u8> {
    fn write_slice(&mut self, src: &[u8]) -> Result<()> {
        self.extend_from_slice(src);
        Ok(())
    }
}

/// A `MemBufMut` adapter which implements [std::io::Write] for the inner value
#[derive(Debug)]
pub struct Writer<'a, B> {
    buf: &'a mut B,
}

impl<'a, B: MemBufMut> Writer<'a, B> {
    /// Create a new Writer from a mut ref to buf
    pub fn new(buf: &'a mut B) -> Self {
        Self { buf }
    }
}

impl<'a, B: MemBufMut> Write for Writer<'a, B> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.buf.write_slice(src).map_err(|e| match &e {
            Error::UnexpectedEof { .. } => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            Error::WouldOverflow { .. } => io::Error::new(io::ErrorKind::WriteZero, e),
        })?;
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for &mut dyn MemBufMut {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.write_slice(src).map_err(|e| match &e {
            Error::UnexpectedEof { .. } => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            Error::WouldOverflow { .. } => io::Error::new(io::ErrorKind::WriteZero, e),
        })?;
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_mut_mem_buf() {
        let hello = b"hello";
        let mut buffer = BytesMut::new();
        buffer.write_u8(8).unwrap();
        buffer.write_u64(u64::MAX - 5).unwrap();
        buffer.write_slice(hello).unwrap();

        assert_eq!(&buffer, buffer.remaining_slice());
        assert_eq!(8, buffer.read_u8().unwrap());
        assert_eq!(u64::MAX - 5, buffer.read_u64().unwrap());
        let mut dst = [0; 5];
        buffer.read_to_slice(&mut dst).unwrap();
        assert_eq!(hello, &dst);

        assert!(buffer.remaining_slice().is_empty());
    }

    #[test]
    fn test_bytes_mut_empty() {
        let mut buffer = BytesMut::new();
        assert!(buffer.remaining_slice().is_empty());
        assert!(matches!(buffer.read_u8(), Err(Error::UnexpectedEof { .. })));
        assert!(matches!(
            buffer.read_u64(),
            Err(Error::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn test_bytes_mem_buf() {
        let mut buffer = Bytes::from_static(b"hello world");
        assert_eq!(b"hello world", buffer.remaining_slice());

        let mut dst = [0; 5];
        buffer.read_to_slice(&mut dst).unwrap();
        assert_eq!(b"hello", &dst);

        assert_eq!(b" world", buffer.remaining_slice());
        buffer.must_advance(1);
        assert_eq!(b"world", buffer.remaining_slice());

        let mut dst = [0; 50];
        assert!(matches!(
            buffer.read_to_slice(&mut dst),
            Err(Error::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn test_slice_mem_buf() {
        let hello = b"hello world";
        let mut buf = &hello[..];

        assert_eq!(hello, buf.remaining_slice());
        let mut dst = [0; 6];
        buf.read_to_slice(&mut dst).unwrap();
        assert_eq!(b"hello ", &dst);
        assert_eq!(b"world", buf.remaining_slice());

        buf.must_advance(1);
        assert_eq!(b"orld", buf.remaining_slice());
    }

    #[test]
    fn test_slice_mem_buf_mut() {
        let mut dst = [b'x'; 11];
        {
            let mut buf = &mut dst[..];

            buf.write_slice(b"abcde").unwrap();
            assert_eq!(b"abcdexxxxxx", &dst);
        }

        {
            let mut buf = &mut dst[..];

            buf.write_slice(b"hello").unwrap();
            buf.write_slice(b" world").unwrap();
            assert_eq!(b"hello world", &dst);
        }

        let mut dst = [0; 3];
        let mut buf = &mut dst[..];
        assert!(matches!(
            buf.write_slice(b"a long long long slice"),
            Err(Error::WouldOverflow { .. })
        ));
    }

    #[test]
    fn test_vec_mem_buf_mut() {
        let mut buf = Vec::new();
        buf.write_slice(b"hello").unwrap();
        assert_eq!(b"hello", &buf[..]);
    }

    #[test]
    fn test_writer_write() {
        let mut buf = Vec::new();
        let mut writer = Writer::new(&mut buf);
        writer.write_all(b"he").unwrap();
        writer.write_all(b"llo").unwrap();
        assert_eq!(b"hello", &buf[..]);
    }

    #[test]
    fn test_writer_overflow() {
        let mut dst = [0; 3];
        let mut buf = &mut dst[..];
        let mut writer = Writer::new(&mut buf);
        assert_eq!(
            io::ErrorKind::WriteZero,
            writer.write_all(b"0123456789").err().unwrap().kind()
        );
    }
}
