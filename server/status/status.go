// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package status

import "sync/atomic"

type Status int32

const (
	StatusWaiting Status = iota
	StatusRunning
	Terminated
)

type ServerStatus struct {
	status Status
}

func NewServerStatus() *ServerStatus {
	return &ServerStatus{
		status: StatusWaiting,
	}
}

func (s *ServerStatus) Set(status Status) {
	atomic.StoreInt32((*int32)(&s.status), int32(status))
}

func (s *ServerStatus) Get() Status {
	return Status(atomic.LoadInt32((*int32)(&s.status)))
}

func (s *ServerStatus) IsHealthy() bool {
	return s.Get() == StatusRunning
}
