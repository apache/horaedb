// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/config"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/grpcservice"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/CeresDB/ceresmeta/server/schedule"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	isClosed int32

	cfg     *config.Config
	etcdCfg *embed.Config

	// The fields below are initialized after Run of server is called.
	hbStreams      *schedule.HeartbeatStreams
	clusterManager cluster.Manager

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
func CreateServer(cfg *config.Config) (*Server, error) {
	etcdCfg, err := cfg.GenEtcdConfig()
	if err != nil {
		return nil, err
	}

	srv := &Server{
		isClosed: 0,

		cfg:     cfg,
		etcdCfg: etcdCfg,
	}

	grpcService := grpcservice.NewService(cfg.GrpcHandleTimeout(), srv)
	etcdCfg.ServiceRegister = func(grpcSrv *grpc.Server) {
		grpcSrv.RegisterService(&metaservicepb.CeresmetaRpcService_ServiceDesc, grpcService)
	}

	return srv, nil
}

// Run runs the services and background jobs.
func (srv *Server) Run(ctx context.Context) error {
	if err := srv.startEtcd(ctx); err != nil {
		return err
	}

	if err := srv.startServer(ctx); err != nil {
		return err
	}

	srv.startBgJobs(ctx)

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

	srv.hbStreams.Close()

	// TODO: release other resources: httpclient, etcd server and so on.
}

func (srv *Server) IsClosed() bool {
	return atomic.LoadInt32(&srv.isClosed) == 1
}

func (srv *Server) startEtcd(ctx context.Context) error {
	etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
	if err != nil {
		return ErrStartEtcd.WithCause(err)
	}

	newCtx, cancel := context.WithTimeout(ctx, srv.cfg.EtcdStartTimeout())
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
	srv.member = member.NewMember("", uint64(etcdSrv.Server.ID()), srv.cfg.NodeName, client, etcdLeaderGetter, srv.cfg.EtcdCallTimeout())
	srv.etcdSrv = etcdSrv
	return nil
}

/// startServer starts involved services.
func (srv *Server) startServer(ctx context.Context) error {
	srv.hbStreams = schedule.NewHeartbeatStreams(ctx)

	if srv.cfg.MaxScanLimit <= 1 {
		return ErrStartServer.WithCausef("scan limit must be greater than 1")
	}

	storage := storage.NewStorageWithEtcdBackend(srv.etcdCli, srv.cfg.StorageRootPath, storage.Options{
		MaxScanLimit: srv.cfg.MaxScanLimit, MinScanLimit: srv.cfg.MinScanLimit,
	})

	manager, err := cluster.NewManagerImpl(storage, srv.etcdCli, srv.hbStreams, srv.cfg.StorageRootPath, srv.cfg.IDAllocatorStep)
	if err != nil {
		return errors.Wrap(err, "start server")
	}
	srv.clusterManager = manager

	log.Info("server started")
	return nil
}

func (srv *Server) startBgJobs(ctx context.Context) {
	var bgJobCtx context.Context
	bgJobCtx, srv.bgJobCancel = context.WithCancel(ctx)

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
func (srv *Server) watchLeader(ctx context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()

	watchCtx := &leaderWatchContext{
		srv,
	}
	watcher := member.NewLeaderWatcher(watchCtx, srv.member, srv.cfg.LeaseTTLSec)

	watcher.Watch(ctx, nil)
}

func (srv *Server) watchEtcdLeaderPriority(_ context.Context) {
	srv.bgJobWg.Add(1)
	defer srv.bgJobWg.Done()
}

//nolint
func (srv *Server) createDefaultCluster(ctx context.Context) {
	leaderResp, err := srv.member.GetLeader(ctx)
	if err != nil {
		log.Warn("get leader failed", zap.Error(err))
	}

	// Create default cluster by the leader.
	if leaderResp.IsLocal {
		cluster, err := srv.clusterManager.CreateCluster(ctx, srv.cfg.DefaultClusterName, uint32(srv.cfg.DefaultClusterNodeCount), uint32(srv.cfg.DefaultClusterReplicationFactor), uint32(srv.cfg.DefaultClusterShardTotal))
		if err != nil {
			log.Warn("create default cluster failed", zap.Error(err))
		} else {
			log.Info("create default cluster succeed", zap.Any("cluster", cluster))
		}
	}
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

func (srv *Server) GetClusterManager() cluster.Manager {
	return srv.clusterManager
}

func (srv *Server) GetLeader(ctx context.Context) (*member.GetLeaderResp, error) {
	return srv.member.GetLeader(ctx)
}

func (srv *Server) BindHeartbeatStream(_ context.Context, node string, sender grpcservice.HeartbeatStreamSender) error {
	srv.hbStreams.Bind(node, sender)
	return nil
}

func (srv *Server) UnbindHeartbeatStream(_ context.Context, node string) error {
	srv.hbStreams.Unbind(node)
	return nil
}

func (srv *Server) ProcessHeartbeat(ctx context.Context, req *metaservicepb.NodeHeartbeatRequest) error {
	return srv.clusterManager.RegisterNode(ctx, req.GetHeader().GetClusterName(), req.GetInfo().GetEndpoint(), req.GetInfo().GetLease())
}
