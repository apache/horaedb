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

// Copyright 2023 The HoraeDB Authors
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

use std::{
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use logger::{info, warn};

#[derive(Default)]
struct State {
    ref_count: u32,
    invalid: bool,
}

pub struct ResourceGuard<'a, T: Send + Sync> {
    resource: &'a T,
    state: Arc<Mutex<State>>,
}

impl<'a, T: Send + Sync> Deref for ResourceGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &'a T {
        self.resource
    }
}

impl<'a, T: Send + Sync> Drop for ResourceGuard<'a, T> {
    fn drop(&mut self) {
        self.state.lock().unwrap().ref_count -= 1;
    }
}

pub struct ResourceKeeper<T: Send + Sync> {
    resource: T,
    state: Arc<Mutex<State>>,

    name: String,
    check_release_interval: Duration,
}

impl<T: Send + Sync> ResourceKeeper<T> {
    pub fn new(name: String, resource: T, check_release_interval: Duration) -> Self {
        Self {
            resource,
            state: Arc::new(Mutex::new(State::default())),

            name,
            check_release_interval,
        }
    }

    pub fn try_acquire(&self) -> Option<ResourceGuard<'_, T>> {
        {
            let mut state = self.state.lock().unwrap();
            if state.invalid {
                return None;
            }
            state.ref_count += 1;
        }

        let guard = ResourceGuard {
            resource: &self.resource,
            state: self.state.clone(),
        };
        Some(guard)
    }

    pub async fn wait_release(&self) {
        // Set the state is invalid to avoid future acquire.
        {
            let mut state = self.state.lock().unwrap();
            state.invalid = true;
        }

        // Wait until all the resource references are dropped.
        let mut wait_cnt = 0;
        while !self.check_released_once() {
            wait_cnt += 1;
            if wait_cnt % 100 == 0 {
                warn!(
                    "Resource {} is still in use, wait for release {wait_cnt} times",
                    self.name
                );
            }
            tokio::time::sleep(self.check_release_interval).await;
        }

        info!(
            "Resource {} is released, waited for {:?}",
            self.name,
            wait_cnt * self.check_release_interval
        );
    }

    #[inline]
    fn check_released_once(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.ref_count == 0
    }
}
