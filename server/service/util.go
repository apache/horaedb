// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package service

import (
	"context"
	"net/url"
	"strings"

	"github.com/CeresDB/ceresmeta/pkg/coderr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrParseURL = coderr.NewCodeError(coderr.Internal, "parse url")
	ErrGRPCDial = coderr.NewCodeError(coderr.Internal, "grpc dial")
)

// GetClientConn returns a gRPC client connection.
func GetClientConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())

	host := addr
	if strings.HasPrefix(addr, "http") {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, ErrParseURL.WithCause(err)
		}
		host = u.Host
	}

	cc, err := grpc.DialContext(ctx, host, opt)
	if err != nil {
		return nil, ErrGRPCDial.WithCause(err)
	}
	return cc, nil
}
