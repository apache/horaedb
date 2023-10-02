// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package assert

import "fmt"

// Assertf panics and prints the appended message if the cond is false.
func Assertf(cond bool, format string, a ...any) {
	if !cond {
		msg := fmt.Sprintf(format, a...)
		panic(msg)
	}
}

// Assertf panics and prints the appended message if the cond is false.
func Assert(cond bool) {
	Assertf(cond, "unexpected case")
}
