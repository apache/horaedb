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
