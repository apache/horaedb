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
