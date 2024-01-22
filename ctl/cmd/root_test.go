/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Forked from https://github.com/apache/incubator-seata-ctl/blob/8427314e04cdc435b925ed41573b37e3addeea34/action/common/args_test.go.

package cmd

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var argsTestCases = []struct {
	input string
	args  []string
	valid bool
}{
	{
		`-a xxx -b yyy -c ' { "a": "b", "c": "d" }' -d -e`,
		[]string{"-a", "xxx", "-b", "yyy", "-c", `{ "a": "b", "c": "d" }`, "-d", "-e"},
		true,
	},
	{
		`-a xxx -b yyy \
-c \
' { \
    "a": "b", \
    "c": "d" \
}' \
-d \
-e`,
		[]string{"-a", "xxx", "-b", "yyy", "-c", `{  "a": "b",  "c": "d"  }`, "-d", "-e"},
		true,
	},
	{
		`-a xxx -b yyy
-c \
' { \
    "a": "b", \
    "c": "d" \
}' \
-d \
-e`,
		[]string{"-a", "xxx", "-b", "yyy"},
		true,
	},
	{
		`-a \
' { \
    "a": "b" \
-b`,
		[]string{},
		false,
	},
}

func TestReadArgs(t *testing.T) {
	var stdin bytes.Buffer
	for _, testCase := range argsTestCases {
		stdin.Reset()
		stdin.Write([]byte(testCase.input))
		if !testCase.valid {
			assert.NotNil(t, ReadArgs(&stdin))
			continue
		}
		assert.Nil(t, ReadArgs(&stdin))
		assert.Equal(t, len(os.Args), len(testCase.args))
		for i := 0; i < len(os.Args); i++ {
			assert.Equal(t, os.Args[i], testCase.args[i])
		}
	}
}
