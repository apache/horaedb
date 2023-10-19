// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"fmt"
	"net/http"
	"time"
)

const defaultReadHeaderTimeout time.Duration = time.Duration(5) * time.Second

// Service is wrapper for http.Server
type Service struct {
	port         int
	readTimeout  time.Duration
	writeTimeout time.Duration

	router *Router
	server http.Server
}

func NewHTTPService(port int, readTimeout time.Duration, writeTimeout time.Duration, router *Router) *Service {
	return &Service{
		port:         port,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		router:       router,
		server: http.Server{
			ReadHeaderTimeout: defaultReadHeaderTimeout,
		},
	}
}

func (s *Service) Start() error {
	s.server.ReadTimeout = s.readTimeout
	s.server.WriteTimeout = s.writeTimeout
	s.server.Addr = fmt.Sprintf(":%d", s.port)
	s.server.Handler = s.router

	return s.server.ListenAndServe()
}

func (s *Service) Stop() error {
	return s.server.Close()
}
