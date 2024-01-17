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

use std::future::Future;

use crate::{JoinHandle, RuntimeRef};

// TODO: maybe we could move this to common_types crate.
#[derive(Copy, Clone, Debug, Default)]
#[repr(u8)]
pub enum Priority {
    #[default]
    High = 0,
    Low = 1,
}

impl Priority {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::High => "high",
            Self::Low => "low",
        }
    }
}

impl TryFrom<u8> for Priority {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Priority::High),
            1 => Ok(Priority::Low),
            _ => Err(format!("Unknown priority, value:{value}")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PriorityRuntime {
    low: RuntimeRef,
    high: RuntimeRef,
}

impl PriorityRuntime {
    pub fn new(low: RuntimeRef, high: RuntimeRef) -> Self {
        Self { low, high }
    }

    pub fn low(&self) -> &RuntimeRef {
        &self.low
    }

    pub fn high(&self) -> &RuntimeRef {
        &self.high
    }

    pub fn choose_runtime(&self, priority: &Priority) -> &RuntimeRef {
        match priority {
            Priority::Low => &self.low,
            Priority::High => &self.high,
        }
    }

    // By default we spawn the future to the higher priority runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.high.spawn(future)
    }

    pub fn spawn_with_priority<F>(&self, future: F, priority: Priority) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match priority {
            Priority::Low => self.low.spawn(future),
            Priority::High => self.high.spawn(future),
        }
    }
}
