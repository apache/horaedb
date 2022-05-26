// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contains all needed macros

/// Define result for given Error type
#[macro_export]
macro_rules! define_result {
    ($t:ty) => {
        pub type Result<T> = std::result::Result<T, $t>;
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_define_result() {
        define_result!(i32);

        fn return_i32_error() -> Result<()> {
            Err(18)
        }

        assert_eq!(Err(18), return_i32_error());
    }
}
