// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package server

import (
	"context"
	"log"
	"sync"

	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

var ErrStartEtcdTimeout = errors.New("Fail to start etcd server in time")

type Server struct {
	ctx         context.Context
	bgJobCtx    context.Context
	bgJobCancel func()
	bgJobWg     sync.WaitGroup

	cfg     *config.Config
	etcdCfg *embed.Config

	// etcd client
	client *clientv3.Client
	// http client
	// httpClient *http.Client
}

// CreateServer creates the server instance without starting any services or background jobs.
func CreateServer(ctx context.Context, cfg *config.Config) (*Server, error) {
	etcdCfg, err := cfg.GenEtcdConfig()
	if err != nil {
		return nil, err
	}

	// TODO: register grpc service in the etcdCfg.ServiceRegister

	srv := &Server{
		ctx:     context.Background(),
		cfg:     cfg,
		etcdCfg: etcdCfg,
	}
	return srv, nil
}

// Run runs the services and background jobs.
func (srv *Server) Run() error {
	if err := srv.startEtcd(); err != nil {
		return err
	}

	if err := srv.startServer(); err != nil {
		return err
	}

	srv.startBgJobs()

	return nil
}

func (srv *Server) Close() {
	srv.stopBgJobs()

	if err := srv.client.Close(); err != nil {
		log.Printf("fail to close client, err:%v", err)
	}

	// TODO: release other resources: httpclient, etcd server and so on.
}

func (srv *Server) startEtcd() error {
	etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
	if err != nil {
		return err
	}

	newCtx, cancel := context.WithTimeout(srv.ctx, srv.cfg.EtcdStartTimeout())
	defer cancel()

	select {
	case <-etcdSrv.Server.ReadyNotify():
	case <-newCtx.Done():
		return ErrStartEtcdTimeout
	}

	// TODO: build etcd client
	endpoints := []string{srv.etcdCfg.ACUrls[0].String()}
	lgc := zap.NewProductionConfig()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: srv.cfg.EtcdDialTimeout(),
		LogConfig:   &lgc,
	})
	if err != nil {
		return err
	}

	srv.client = client
	return nil
}

/// startServer starts the http/grpc services.
func (srv *Server) startServer() error {
	return nil
}

func (srv *Server) startBgJobs() {
	srv.bgJobCtx, srv.bgJobCancel = context.WithCancel(srv.ctx)

	srv.bgJobWg = sync.WaitGroup{}
	srv.bgJobWg.Add(2)
	go srv.watchLeaderSwitch()
	go srv.watchEtcdLeaderPriority()
}

func (srv *Server) stopBgJobs() {
	srv.bgJobCancel()
	srv.bgJobWg.Wait()
}

func (srv *Server) watchLeaderSwitch() {
	defer srv.bgJobWg.Done()
}

func (srv *Server) watchEtcdLeaderPriority() {
	defer srv.bgJobWg.Done()
}
