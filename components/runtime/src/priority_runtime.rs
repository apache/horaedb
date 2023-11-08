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

use std::future::Future;

use crate::{JoinHandle, RuntimeRef};

#[derive(Clone, Debug)]
pub struct PriorityRuntime {
    // The runtime that is currently running.
    lower: RuntimeRef,
    higher: RuntimeRef,
}

pub enum Priority {
    Lower,
    Higher,
}

impl PriorityRuntime {
    pub fn new(lower: RuntimeRef, higher: RuntimeRef) -> Self {
        Self { lower, higher }
    }

    pub fn lower(&self) -> &RuntimeRef {
        &self.lower
    }

    pub fn higher(&self) -> &RuntimeRef {
        &self.higher
    }

    // By default we spawn the future to the higher priority runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.higher.spawn(future)
    }

    pub fn spawn_with_priority<F>(&self, future: F, priority: Priority) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match priority {
            Priority::Lower => self.lower.spawn(future),
            Priority::Higher => self.higher.spawn(future),
        }
    }
}
