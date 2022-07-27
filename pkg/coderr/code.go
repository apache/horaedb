// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coderr

import "net/http"

type Code int

const (
	InvalidParams Code = http.StatusBadRequest
	Internal           = http.StatusInternalServerError
	NotFound           = http.StatusNotFound
	BadRequest         = http.StatusBadRequest

	// HTTPCodeUpperBound is a bound under which any Code should have the same meaning with the http status code.
	HTTPCodeUpperBound = Code(1000)
	PrintHelpUsage     = 1001
)

// ToHTTPCode converts the Code to http code.
// The Code below the HTTPCodeUpperBound has the same meaning as the http status code. However, for the other codes, we
// should define the conversion rules by ourselves.
func (c Code) ToHTTPCode() int {
	if c < HTTPCodeUpperBound {
		return int(c)
	}

	// TODO: use switch to convert the code to http code.
	return int(c)
}
