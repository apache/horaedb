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

package coderr

import (
	"fmt"

	"github.com/pkg/errors"
)

var _ CodeError = &codeError{code: 0, desc: "", cause: nil}

// CodeError is an error with code.
type CodeError interface {
	error
	Code() Code
	// WithCausef should generate a new CodeError instance with the provided cause details.
	WithCausef(format string, a ...any) CodeError
	// WithCause should generate a new CodeError instance with the provided cause details.
	WithCause(cause error) CodeError
}

// Is checks whether the cause of `err` is the kind of error specified by the `expectCode`.
// Returns false if the cause of `err` is not CodeError.
func Is(err error, expectCode Code) bool {
	code, b := GetCauseCode(err)
	if b && code == expectCode {
		return true
	}

	return false
}

func GetCauseCode(err error) (Code, bool) {
	if err == nil {
		return Invalid, false
	}

	cause := errors.Cause(err)
	cerr, ok := cause.(CodeError)
	if !ok {
		return Invalid, false
	}
	return cerr.Code(), true
}

// NewCodeError creates a base CodeError definition.
// The provided code should be defined in the code.go in this package.
func NewCodeError(code Code, desc string) CodeError {
	return &codeError{
		code:  code,
		desc:  desc,
		cause: nil,
	}
}

// codeError is the default implementation of CodeError.
type codeError struct {
	code  Code
	desc  string
	cause error
}

func (e *codeError) Error() string {
	return fmt.Sprintf("(#%d)%s, cause:%+v", e.code, e.desc, e.cause)
}

func (e *codeError) Code() Code {
	return e.code
}

func (e *codeError) WithCausef(format string, a ...any) CodeError {
	errMsg := fmt.Sprintf(format, a...)
	causeWithStack := errors.WithStack(errors.New(errMsg))
	return &codeError{
		code:  e.code,
		desc:  e.desc,
		cause: causeWithStack,
	}
}

func (e *codeError) WithCause(cause error) CodeError {
	causeWithStack := errors.WithStack(cause)
	return &codeError{
		code:  e.code,
		desc:  e.desc,
		cause: causeWithStack,
	}
}
