/*
 * Copyright 2022 The CeresDB Authors
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

package member

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrResetLeader        = coderr.NewCodeError(coderr.Internal, "reset leader by deleting leader key")
	ErrGetLeader          = coderr.NewCodeError(coderr.Internal, "get leader by querying leader key")
	ErrTxnPutLeader       = coderr.NewCodeError(coderr.Internal, "put leader key in txn")
	ErrMultipleLeader     = coderr.NewCodeError(coderr.Internal, "multiple leaders found")
	ErrInvalidLeaderValue = coderr.NewCodeError(coderr.Internal, "invalid leader value")
	ErrMarshalMember      = coderr.NewCodeError(coderr.Internal, "marshal member information")
	ErrGrantLease         = coderr.NewCodeError(coderr.Internal, "grant lease")
	ErrRevokeLease        = coderr.NewCodeError(coderr.Internal, "revoke lease")
	ErrCloseLease         = coderr.NewCodeError(coderr.Internal, "close lease")
)
