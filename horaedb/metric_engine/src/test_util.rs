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
macro_rules! arrow_schema {
    ($(($field_name:expr, $data_type:ident)),* $(,)?) => {{
        let fields = vec![
            $(
                arrow::datatypes::Field::new($field_name, arrow::datatypes::DataType::$data_type, true),
            )*
        ];
        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_arrow_schema_macro() {
        let schema = arrow_schema![("a", UInt8), ("b", UInt8), ("c", UInt8), ("d", UInt8),];

        let expected_names = ["a", "b", "c", "d"];
        for (i, f) in schema.fields().iter().enumerate() {
            assert_eq!(f.name(), expected_names[i]);
        }
    }
}
