// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_export]
macro_rules! compare_primitive_columns {
    ($lhs_col:expr, $rhs_col:expr, $lhs_idx:expr, $rhs_idx:expr, $($type:ty),+) => {
        $(
            if let Some(lhs_col) = $lhs_col.as_primitive_opt::<$type>() {
                let rhs_col = $rhs_col.as_primitive::<$type>();
                if !lhs_col.value($lhs_idx).eq(&rhs_col.value($rhs_idx)) {
                    return false;
                }
            }
        )+
    };
}

/// Util for working with anyhow + thiserror
/// Works like anyhow's [ensure](https://docs.rs/anyhow/latest/anyhow/macro.ensure.html)
/// But return `Return<T, ErrorFromAnyhow>`
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:literal) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(anyhow::anyhow!($fmt, $($arg)*).into());
        }
    };
}
