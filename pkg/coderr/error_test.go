// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorStack(t *testing.T) {
	r := require.New(t)
	cerr := NewCodeError(Internal, "test internal error")
	err := cerr.WithCausef("failed reason:%s", "for test")
	errDesc := fmt.Sprintf("%v", err)
	expectErrDesc := "(#500)test internal error, cause:failed reason:for test\ngithub.com/CeresDB/ceresmeta/pkg/coderr.(*codeError).WithCausef\n\t/Users/xkwei/Code/github/ceresmeta/pkg/coderr/error.go:72\ngithub.com/CeresDB/ceresmeta/pkg/coderr.TestErrorStack\n\t/Users/xkwei/Code/github/ceresmeta/pkg/coderr/error_test.go:15\ntesting.tRunner\n\t/usr/local/go/src/testing/testing.go:1446\nruntime.goexit\n\t/usr/local/go/src/runtime/asm_amd64.s:1594\ngithub.com/CeresDB/ceresmeta/pkg/coderr.(*codeError).WithCausef\n\t/Users/xkwei/Code/github/ceresmeta/pkg/coderr/error.go:72\ngithub.com/CeresDB/ceresmeta/pkg/coderr.TestErrorStack\n\t/Users/xkwei/Code/github/ceresmeta/pkg/coderr/error_test.go:15\ntesting.tRunner\n\t/usr/local/go/src/testing/testing.go:1446\nruntime.goexit\n\t/usr/local/go/src/runtime/asm_amd64.s:1594"

	r.Equal(expectErrDesc, errDesc)
}
