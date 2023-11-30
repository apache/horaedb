/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package coderr

import "net/http"

type Code int

const (
	Invalid           Code = -1
	Ok                     = 0
	InvalidParams          = http.StatusBadRequest
	BadRequest             = http.StatusBadRequest
	NotFound               = http.StatusNotFound
	TooManyRequests        = http.StatusTooManyRequests
	Internal               = http.StatusInternalServerError
	ErrNotImplemented      = http.StatusNotImplemented

	// HTTPCodeUpperBound is a bound under which any Code should have the same meaning with the http status code.
	HTTPCodeUpperBound   = Code(1000)
	PrintHelpUsage       = 1001
	ClusterAlreadyExists = 1002
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
