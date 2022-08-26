// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"net/url"

	"github.com/CeresDB/ceresdbproto/pkg/commonpb"
	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// getClientConn returns a gRPC client connection.
func getClientConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())

	u, err := url.Parse(addr)
	if err != nil {
		return nil, ErrParseURL.WithCause(err)
	}

	cc, err := grpc.DialContext(ctx, u.Host, opt)
	if err != nil {
		return nil, ErrGRPCDial.WithCause(err)
	}
	return cc, nil
}

func okResponseHeader() *commonpb.ResponseHeader {
	return responseHeader(nil, "")
}

func responseHeader(err error, msg string) *commonpb.ResponseHeader {
	if err == nil {
		return &commonpb.ResponseHeader{Code: coderr.Ok, Error: msg}
	}

	code, ok := coderr.GetCauseCode(err)
	if ok {
		return &commonpb.ResponseHeader{Code: uint32(code), Error: msg + err.Error()}
	}

	return &commonpb.ResponseHeader{Code: coderr.Internal, Error: msg + err.Error()}
}
