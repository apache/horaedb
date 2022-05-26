// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Load balancer

use common_util::define_result;
use rand::Rng;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Meta Addresses empty.\nBacktrace:\n{}", backtrace))]
    MetaAddressesEmpty { backtrace: Backtrace },
}

define_result!(Error);

pub trait LoadBalancer {
    fn select<'a>(&self, addresses: &'a [String]) -> Result<&'a String>;
}

pub struct RandomLoadBalancer;

impl LoadBalancer for RandomLoadBalancer {
    fn select<'a>(&self, addresses: &'a [String]) -> Result<&'a String> {
        if addresses.is_empty() {
            return MetaAddressesEmpty.fail();
        }

        let len = addresses.len();
        if len == 1 {
            return Ok(&addresses[0]);
        }
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0, len);

        Ok(&addresses[idx])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_random_loadbalancer() {
        let lb = RandomLoadBalancer;
        let addresses = vec![
            "127.0.0.1:8080".to_string(),
            "127.0.0.2:8080".to_string(),
            "127.0.0.3:8080".to_string(),
            "127.0.0.4:8080".to_string(),
            "127.0.0.5:8080".to_string(),
        ];
        for _idx in 0..100 {
            let addr = lb.select(&addresses).unwrap();
            assert!(addresses.contains(addr));
        }

        // Empty case
        assert!(lb.select(&[]).is_err());

        let addresses = ["127.0.0.1:5000".to_string()];
        assert_eq!(&addresses[0], lb.select(&addresses).unwrap());
    }
}
