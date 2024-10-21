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

module go-sdk-test

go 1.21

require github.com/apache/incubator-horaedb-client-go v1.2.0

require (
	github.com/CeresDB/horaedbproto/golang v0.0.0-20231129131648-5d5d868218c3 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.15.14 // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.56.3 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)

// TODO: remove this when a new version is released.
replace github.com/apache/incubator-horaedb-client-go => github.com/CeresDB/horaedb-client-go v1.2.0
