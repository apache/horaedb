// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type GenericResult<T> = std::result::Result<T, GenericError>;
