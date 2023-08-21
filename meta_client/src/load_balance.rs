// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Load balancer

use macros::define_result;
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
