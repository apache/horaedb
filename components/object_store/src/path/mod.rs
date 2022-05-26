// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

pub mod file;
pub mod parsed;
pub mod parts;

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";

/// Universal interface for handling paths and locations for objects and
/// directories in the object store.
///
///
/// Deliberately does not implement `Display` or `ToString`!
pub trait ObjectStorePath:
    std::fmt::Debug + Clone + PartialEq + Eq + Send + Sync + 'static
{
    /// Set the file name of this path
    fn set_file_name(&mut self, part: impl Into<String>);

    /// Add a part to the end of the path's directories, encoding any restricted
    /// characters.
    fn push_dir(&mut self, part: impl Into<String>);

    /// Push a bunch of parts as directories in one go.
    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>);

    /// Like `std::path::Path::display, converts an `ObjectStorePath` to a
    /// `String` suitable for printing; not suitable for sending to
    /// APIs.
    fn display(&self) -> String;
}
