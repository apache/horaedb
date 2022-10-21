// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorStack(t *testing.T) {
	r := require.New(t)
	cerr := NewCodeError(Internal, "test internal error")
	err := cerr.WithCausef("failed reason:%s", "for test")
	errDesc := fmt.Sprintf("%s", err)
	expectErrDesc := "ceresmeta/pkg/coderr/error_test.go:"

	r.True(strings.Contains(errDesc, expectErrDesc), "actual errDesc:%s", errDesc)
}
