// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/member"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

type Server struct {
	ctx      context.Context
	isClosed int32

	cfg     *config.Config
	etcdCfg *embed.Config

	// The fields below are initialized after Run of server is called.

	// member describes membership in ceresmeta cluster.
	member  *member.Member
	etcdCli *clientv3.Client
	etcdSrv *embed.Etcd

	// bgJobWg can be used to join with the background jobs.
	bgJobWg sync.WaitGroup
	// bgJobCancel can be used to cancel all pending background jobs.
	bgJobCancel func()
}

// CreateServer creates the server instance without starting any services or background jobs.
func CreateServer(ctx context.Context, cfg *config.Config) (*Server, error) {
	etcdCfg, err := cfg.GenEtcdConfig()
	if err != nil {
		return nil, err
	}

	// TODO: register grpc service in the etcdCfg.ServiceRegister

	srv := &Server{
		ctx:      context.Background(),
		isClosed: 0,

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
	atomic.StoreInt32(&srv.isClosed, 1)
	srv.stopBgJobs()

	if srv.etcdCli != nil {
		err := srv.etcdCli.Close()
		if err != nil {
			log.Error("fail to close etcdCli", zap.Error(err))
		}
	}

	// TODO: release other resources: httpclient, etcd server and so on.
}

func (srv *Server) IsClosed() bool {
	return atomic.LoadInt32(&srv.isClosed) == 1
}

func (srv *Server) startEtcd() error {
	etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
	if err != nil {
		return ErrStartEtcd.WithCause(err)
	}

	newCtx, cancel := context.WithTimeout(srv.ctx, srv.cfg.EtcdStartTimeout())
	defer cancel()

	select {
	case <-etcdSrv.Server.ReadyNotify():
	case <-newCtx.Done():
		return ErrStartEtcdTimeout.WithCausef("timeout is:%v", srv.cfg.EtcdStartTimeout())
	}

	endpoints := []string{srv.etcdCfg.ACUrls[0].String()}
	lgc := log.GetLoggerConfig()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: srv.cfg.EtcdCallTimeout(),
		LogConfig:   lgc,
	})
	if err != nil {
		return ErrCreateEtcdClient.WithCause(err)
	}

	srv.etcdCli = client
	etcdLeaderGetter := &etcdutil.LeaderGetterWrapper{Server: etcdSrv.Server}
	srv.member = member.NewMember("", uint64(etcdSrv.Server.ID()), "", client, etcdLeaderGetter, srv.cfg.EtcdCallTimeout())
	srv.etcdSrv = etcdSrv
	return nil
}

/// startServer starts the http/grpc services.
func (srv *Server) startServer() error {
	return nil
}

func (srv *Server) startBgJobs() {
	var bgJobCtx context.Context
	bgJobCtx, srv.bgJobCancel = context.WithCancel(srv.ctx)

	go srv.watchLeader(bgJobCtx)
	go srv.watchEtcdLeaderPriority(bgJobCtx)
}

func (srv *Server) stopBgJobs() {
	srv.bgJobCancel()
	srv.bgJobWg.Wait()
}

// watchLeader watches whether the leader of the cluster exists.
// Every node campaigns the leadership if it finds the leader is offline and the leader should keep the leadership after
// election. And Keep the leader node also be the leader of the etcd cluster during election.
func (srv *Server) watchLeader(_ context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()

	watchCtx := &leaderWatchContext{
		srv,
	}
	watcher := member.NewLeaderWatcher(watchCtx, srv.member, srv.cfg.LeaseTTLSec)

	watcher.Watch(srv.ctx)
}

func (srv *Server) watchEtcdLeaderPriority(_ context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()
}

type leaderWatchContext struct {
	srv *Server
}

func (ctx *leaderWatchContext) ShouldStop() bool {
	return ctx.srv.IsClosed()
}

func (ctx *leaderWatchContext) EtcdLeaderID() uint64 {
	return ctx.srv.etcdSrv.Server.Lead()
}
