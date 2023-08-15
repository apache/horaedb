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

//! Row utils

use common_types::{datum::Datum, row::Row};

pub fn new_row_6<C0, C1, C2, C3, C4, C5>(data: (C0, C1, C2, C3, C4, C5)) -> Row
where
    C0: Into<Datum>,
    C1: Into<Datum>,
    C2: Into<Datum>,
    C3: Into<Datum>,
    C4: Into<Datum>,
    C5: Into<Datum>,
{
    let cols = vec![
        data.0.into(),
        data.1.into(),
        data.2.into(),
        data.3.into(),
        data.4.into(),
        data.5.into(),
    ];

    Row::from_datums(cols)
}

pub fn assert_row_eq_6<C0, C1, C2, C3, C4, C5>(data: (C0, C1, C2, C3, C4, C5), row: Row)
where
    C0: Into<Datum>,
    C1: Into<Datum>,
    C2: Into<Datum>,
    C3: Into<Datum>,
    C4: Into<Datum>,
    C5: Into<Datum>,
{
    let expect_row = new_row_6(data);
    assert_eq!(expect_row, row);
}

pub fn new_row_8<C0, C1, C2, C3, C4, C5, C6, C7>(data: (C0, C1, C2, C3, C4, C5, C6, C7)) -> Row
where
    C0: Into<Datum>,
    C1: Into<Datum>,
    C2: Into<Datum>,
    C3: Into<Datum>,
    C4: Into<Datum>,
    C5: Into<Datum>,
    C6: Into<Datum>,
    C7: Into<Datum>,
{
    let cols = vec![
        data.0.into(),
        data.1.into(),
        data.2.into(),
        data.3.into(),
        data.4.into(),
        data.5.into(),
        data.6.into(),
        data.7.into(),
    ];

    Row::from_datums(cols)
}

pub fn new_rows_6<C0, C1, C2, C3, C4, C5>(data: &[(C0, C1, C2, C3, C4, C5)]) -> Vec<Row>
where
    C0: Into<Datum> + Clone,
    C1: Into<Datum> + Clone,
    C2: Into<Datum> + Clone,
    C3: Into<Datum> + Clone,
    C4: Into<Datum> + Clone,
    C5: Into<Datum> + Clone,
{
    data.iter().cloned().map(new_row_6).collect()
}

#[allow(clippy::type_complexity)]
pub fn new_rows_8<C0, C1, C2, C3, C4, C5, C6, C7>(
    data: &[(C0, C1, C2, C3, C4, C5, C6, C7)],
) -> Vec<Row>
where
    C0: Into<Datum> + Clone,
    C1: Into<Datum> + Clone,
    C2: Into<Datum> + Clone,
    C3: Into<Datum> + Clone,
    C4: Into<Datum> + Clone,
    C5: Into<Datum> + Clone,
    C6: Into<Datum> + Clone,
    C7: Into<Datum> + Clone,
{
    data.iter().cloned().map(new_row_8).collect()
}
