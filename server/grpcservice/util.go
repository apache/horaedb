// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"net/url"

	"github.com/CeresDB/ceresdbproto/pkg/commonpb"
	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"google.golang.org/grpc"
)

// getClientConn returns a gRPC client connection.
func getClientConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, ErrParseURL.WithCause(err)
	}

	cc, err := grpc.DialContext(ctx, u.Host)
	if err != nil {
		return nil, ErrGRPCDial.WithCause(err)
	}
	return cc, nil
}

func okResponseHeader() *commonpb.ResponseHeader {
	return responseHeader(nil, "")
}

func responseHeader(err error, errMsg string) *commonpb.ResponseHeader {
	code, b := coderr.GetCauseCode(err)
	if !b {
		code = coderr.Internal
	}
	return &commonpb.ResponseHeader{Code: uint32(code), Error: errMsg + err.Error()}
}
