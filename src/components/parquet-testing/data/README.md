<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Test data files for Parquet compatibility and regression testing

| File                                         | Description                                                                                                                                                      |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delta_byte_array.parquet                     | string columns with DELTA_BYTE_ARRAY encoding. See [delta_byte_array.md](delta_byte_array.md) for details.                                                       |
| delta_length_byte_array.parquet              | string columns with DELTA_LENGTH_BYTE_ARRAY encoding.                                                                                                            |
| delta_binary_packed.parquet                  | INT32 and INT64 columns with DELTA_BINARY_PACKED encoding. See [delta_binary_packed.md](delta_binary_packed.md) for details.                                     |
| delta_encoding_required_column.parquet       | required INT32 and STRING columns with delta encoding. See [delta_encoding_required_column.md](delta_encoding_required_column.md) for details.                   |
| delta_encoding_optional_column.parquet       | optional INT64 and STRING columns with delta encoding. See [delta_encoding_optional_column.md](delta_encoding_optional_column.md) for details.                   |
| nested_structs.rust.parquet                  | Used to test that the Rust Arrow reader can lookup the correct field from a nested struct. See [ARROW-11452](https://issues.apache.org/jira/browse/ARROW-11452)  |
| data_index_bloom_encoding_stats.parquet | optional STRING column. Contains optional metadata: bloom filters, column index, offset index and encoding stats.                                                |
| null_list.parquet                       | an empty list. Generated from this json `{"emptylist":[]}` and for the purposes of testing correct read/write behaviour of this base case.                       |
| alltypes_tiny_pages.parquet             | small page sizes with dictionary encoding with page index from [impala](https://github.com/apache/impala/tree/master/testdata/data/alltypes_tiny_pages.parquet). |
| alltypes_tiny_pages_plain.parquet       | small page sizes with plain encoding with page index [impala](https://github.com/apache/impala/tree/master/testdata/data/alltypes_tiny_pages.parquet).           |
| rle_boolean_encoding.parquet            | option boolean columns with RLE encoding                                                                                                                         |

TODO: Document what each file is in the table above.

## Encrypted Files

Tests files with .parquet.encrypted suffix are encrypted using Parquet Modular Encryption.

A detailed description of the Parquet Modular Encryption specification can be found here:
```
 https://github.com/apache/parquet-format/blob/encryption/Encryption.md
```

Following are the keys and key ids (when using key\_retriever) used to encrypt the encrypted columns and footer in the all the encrypted files:
* Encrypted/Signed Footer:
  * key:   {0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5}
  * key_id: "kf"
* Encrypted column named double_field:
  * key:  {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,0}
  * key_id: "kc1"
* Encrypted column named float_field:
  * key: {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,1}
  * key_id: "kc2"

The following files are encrypted with AAD prefix "tester":
1. encrypt\_columns\_and\_footer\_disable\_aad\_storage.parquet.encrypted
2. encrypt\_columns\_and\_footer\_aad.parquet.encrypted


A sample that reads and checks these files can be found at the following tests:
```
cpp/src/parquet/encryption-read-configurations-test.cc
cpp/src/parquet/test-encryption-util.h
```
