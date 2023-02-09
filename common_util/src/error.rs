// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type GenericResult<T> = std::result::Result<T, GenericError>;

pub trait BoxError {
    type Item;

    fn box_err(self) -> Result<Self::Item, GenericError>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> BoxError for Result<T, E> {
    type Item = T;

    #[inline(always)]
    fn box_err(self) -> Result<Self::Item, GenericError> {
        self.map_err(|e| Box::new(e) as _)
    }
}
