// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	defaultGrpcHandleTimeoutMs int64 = 10 * 1000
	defaultEtcdStartTimeoutMs  int64 = 10 * 1000
	defaultCallTimeoutMs             = 5 * 1000
	defaultEtcdLeaseTTLSec           = 10

	defaultNodeNamePrefix          = "ceresmeta"
	defaultDataDir                 = "/tmp/ceresmeta/data"
	defaultWalDir                  = "/tmp/ceresmeta/wal"
	defaultClientUrls              = "http://127.0.0.1:2379"
	defaultPeerUrls                = "http://127.0.0.1:2380"
	defaultInitialClusterState     = embed.ClusterStateFlagNew
	defaultInitialClusterToken     = "ceresmeta-cluster" //#nosec G101
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"

	defaultTickIntervalMs    int64 = 500
	defaultElectionTimeoutMs       = 3000
	defaultQuotaBackendBytes       = 8 * 1024 * 1024 * 1024 // 8GB

	defaultMaxRequestBytes uint = 2 * 1024 * 1024 // 2MB
)

type Config struct {
	Log log.Config `toml:"log" json:"log"`

	GrpcHandleTimeoutMs int64 `toml:"grpc-handle-timeout-ms" json:"grpc-handle-timeout-ms"`
	EtcdStartTimeoutMs  int64 `toml:"etcd-start-timeout-ms" json:"etcd-start-timeout-ms"`
	EtcdCallTimeoutMs   int64 `toml:"etcd-call-timeout-ms" json:"etcd-call-timeout-ms"`

	LeaseTTLSec int64 `toml:"lease-sec" json:"lease-sec"`

	NodeName            string `toml:"node-name" json:"node-name"`
	DataDir             string `toml:"data-dir" json:"data-dir"`
	WalDir              string `toml:"wal-dir" json:"wal-dir"`
	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	InitialClusterToken string `toml:"initial-cluster-token" json:"initial-cluster-token"`
	// TickInterval is the interval for etcd Raft tick.
	TickIntervalMs    int64 `toml:"tick-interval-ms" json:"tick-interval-ms"`
	ElectionTimeoutMs int64 `toml:"election-timeout-ms" json:"election-timeout-ms"`
	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes int64 `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode" json:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention" json:"auto-compaction-retention-v2"`
	MaxRequestBytes         uint   `toml:"max-request-bytes" json:"max-request-bytes"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`
}

func (c *Config) GrpcHandleTimeout() time.Duration {
	return time.Duration(c.GrpcHandleTimeoutMs) * time.Millisecond
}

func (c *Config) EtcdStartTimeout() time.Duration {
	return time.Duration(c.EtcdStartTimeoutMs) * time.Millisecond
}

func (c *Config) EtcdCallTimeout() time.Duration {
	return time.Duration(c.EtcdCallTimeoutMs) * time.Millisecond
}

// ValidateAndAdjust validates the config fields and adjusts some fields which should be adjusted.
// Return error if any field is invalid.
func (c *Config) ValidateAndAdjust() error {
	return nil
}

func (c *Config) GenEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()

	cfg.Name = c.NodeName
	cfg.Dir = c.DataDir
	cfg.WalDir = c.WalDir
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.InitialClusterToken = c.InitialClusterToken
	cfg.EnablePprof = true
	cfg.TickMs = uint(c.TickIntervalMs)
	cfg.ElectionMs = uint(c.ElectionTimeoutMs)
	cfg.AutoCompactionMode = c.AutoCompactionMode
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = c.QuotaBackendBytes
	cfg.MaxRequestBytes = c.MaxRequestBytes

	var err error
	cfg.LPUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = parseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = parseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.SetupGlobalLoggers()
	return cfg, nil
}

// Parser builds the config from the flags.
type Parser struct {
	flagSet *flag.FlagSet
	cfg     *Config
}

func (p *Parser) Parse(arguments []string) (*Config, error) {
	if err := p.flagSet.Parse(arguments); err != nil {
		return nil, ErrInvalidCommandArgs.WithCausef("original arguments:%v, parse err:%v", arguments, err)
	}

	// TODO: support loading config from file.

	return p.cfg, nil
}

func makeDefaultNodeName() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", ErrRetrieveHostname.WithCause(err)
	}

	return fmt.Sprintf("%s-%s", defaultNodeNamePrefix, host), nil
}

func makeDefaultInitialCluster(nodeName string) string {
	return fmt.Sprintf("%s=%s", nodeName, defaultPeerUrls)
}

func MakeConfigParser() (*Parser, error) {
	fs, cfg := flag.NewFlagSet("meta", flag.ContinueOnError), &Config{}
	builder := &Parser{
		flagSet: fs,
		cfg:     cfg,
	}

	fs.StringVar(&cfg.Log.Level, "log-level", log.DefaultLogLevel, "level of the log")
	fs.StringVar(&cfg.Log.File, "log-file", log.DefaultLogFile, "file for log output")

	fs.Int64Var(&cfg.GrpcHandleTimeoutMs, "grpc-handle-timeout-ms", defaultGrpcHandleTimeoutMs, "timeout for handling grpc requests")
	fs.Int64Var(&cfg.EtcdStartTimeoutMs, "etcd-start-timeout-ms", defaultEtcdStartTimeoutMs, "timeout for starting etcd server")
	fs.Int64Var(&cfg.EtcdCallTimeoutMs, "etcd-dial-timeout-ms", defaultCallTimeoutMs, "timeout for dialing etcd server")
	fs.Int64Var(&cfg.LeaseTTLSec, "lease-ttl-sec", defaultEtcdLeaseTTLSec, "ttl of etcd key lease (suggest 10s)")

	defaultNodeName, err := makeDefaultNodeName()
	if err != nil {
		return nil, err
	}
	fs.StringVar(&cfg.NodeName, "node-name", defaultNodeName, "member name of this node in the cluster")

	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "data directory for the etcd server")
	fs.StringVar(&cfg.WalDir, "wal-dir", defaultWalDir, "wal directory for the etcd server")

	defaultInitialCluster := makeDefaultInitialCluster(defaultNodeName)
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", defaultInitialCluster, "members in the initial etcd cluster")
	fs.StringVar(&cfg.InitialClusterState, "initial-cluster-state", defaultInitialClusterState, "state of the initial etcd cluster")
	fs.StringVar(&cfg.InitialClusterToken, "initial-cluster-token", defaultInitialClusterToken, "token of the initial etcd cluster")

	fs.StringVar(&cfg.ClientUrls, "client-urls", defaultClientUrls, "url for client traffic")
	fs.StringVar(&cfg.AdvertiseClientUrls, "advertise-client-urls", defaultClientUrls, "advertise url for client traffic (default '${client-urls}')")
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "url for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", defaultPeerUrls, "advertise url for peer traffic (default '${peer-urls}')")

	fs.Int64Var(&cfg.TickIntervalMs, "tick-interval-ms", defaultTickIntervalMs, "tick interval of the etcd server")
	fs.Int64Var(&cfg.ElectionTimeoutMs, "election-timeout-ms", defaultElectionTimeoutMs, "election timeout of the etcd server")

	fs.Int64Var(&cfg.QuotaBackendBytes, "quota-backend-bytes", defaultQuotaBackendBytes, "alarming threshold for too much memory consumption")
	fs.StringVar(&cfg.AutoCompactionMode, "auto-compaction-mode", defaultCompactionMode, "mode of auto compaction of etcd server")
	fs.StringVar(&cfg.AutoCompactionRetention, "auto-compaction-retention", defaultAutoCompactionRetention, "retention for auto compaction(works only if auto-compaction-mode is periodic)")
	fs.UintVar(&cfg.MaxRequestBytes, "max-request-bytes", defaultMaxRequestBytes, "max bytes of requests received by etcd server")

	return builder, nil
}
