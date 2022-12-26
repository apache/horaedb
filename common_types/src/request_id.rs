// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Request id.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy)]
pub struct RequestId(u64);

impl RequestId {
    /// Acquire next request id.
    pub fn next_id() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        Self(id)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id() {
        let id = RequestId::next_id();
        assert_eq!(1, id.0);
        let id = RequestId::next_id();
        assert_eq!(2, id.0);

        assert_eq!("2", id.to_string());
    }
}
