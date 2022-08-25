// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import (
	"fmt"

	"github.com/pkg/errors"
)

var _ CodeError = &codeError{}

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
		code: code,
		desc: desc,
	}
}

// codeError is the default implementation of CodeError.
type codeError struct {
	code  Code
	desc  string
	cause string
}

func (e *codeError) Error() string {
	return fmt.Sprintf("(#%d)%s, cause:%s", e.code, e.desc, e.cause)
}

func (e *codeError) Code() Code {
	return e.code
}

func (e *codeError) WithCausef(format string, a ...any) CodeError {
	return &codeError{
		code:  e.code,
		desc:  e.desc,
		cause: fmt.Sprintf(format, a...),
	}
}

func (e *codeError) WithCause(cause error) CodeError {
	return &codeError{
		code:  e.code,
		desc:  e.desc,
		cause: cause.Error(),
	}
}
