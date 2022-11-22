// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/pelletier/go-toml/v2"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	defaultGrpcHandleTimeoutMs int64 = 10 * 1000
	defaultEtcdStartTimeoutMs  int64 = 10 * 1000
	defaultCallTimeoutMs             = 5 * 1000
	defaultEtcdLeaseTTLSec           = 10

	defaultNodeNamePrefix          = "ceresmeta"
	defaultDataDir                 = "/tmp/ceresmeta/data"
	defaultWalDir                  = "/tmp/ceresmeta/wal"
	defaultRootPath                = "/rootPath"
	defaultClientUrls              = "http://0.0.0.0:2379"
	defaultPeerUrls                = "http://0.0.0.0:2380"
	defaultInitialClusterState     = embed.ClusterStateFlagNew
	defaultInitialClusterToken     = "ceresmeta-cluster" //#nosec G101
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"

	defaultTickIntervalMs    int64 = 500
	defaultElectionTimeoutMs       = 3000
	defaultQuotaBackendBytes       = 8 * 1024 * 1024 * 1024 // 8GB

	defaultMaxRequestBytes uint = 2 * 1024 * 1024 // 2MB

	defaultMaxScanLimit    int  = 100
	defaultMinScanLimit    int  = 20
	defaultIDAllocatorStep uint = 20

	defaultClusterName              = "defaultCluster"
	defaultClusterNodeCount         = 2
	defaultClusterReplicationFactor = 1
	defaultClusterShardTotal        = 8

	defaultHTTPPort = 8080
)

// Config is server start config, it has three input modes:
// 1. toml config file
// 2. env variables
// Their loading has priority, and low priority configurations will be overwritten by high priority configurations.
// The priority from high to low is: env variables > toml config file.
type Config struct {
	Log     log.Config `toml:"log"`
	EtcdLog log.Config `toml:"etcd-log"`

	GrpcHandleTimeoutMs int64 `toml:"grpc-handle-timeout-ms"`
	EtcdStartTimeoutMs  int64 `toml:"etcd-start-timeout-ms"`
	EtcdCallTimeoutMs   int64 `toml:"etcd-call-timeout-ms"`

	LeaseTTLSec int64 `toml:"lease-sec"`

	NodeName            string `toml:"node-name"`
	DataDir             string `toml:"data-dir"`
	WalDir              string `toml:"wal-dir"`
	StorageRootPath     string `toml:"storage-root-path"`
	InitialCluster      string `toml:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state"`
	InitialClusterToken string `toml:"initial-cluster-token"`
	// TickInterval is the interval for etcd Raft tick.
	TickIntervalMs    int64 `toml:"tick-interval-ms"`
	ElectionTimeoutMs int64 `toml:"election-timeout-ms"`
	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes int64 `toml:"quota-backend-bytes"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention"`
	MaxRequestBytes         uint   `toml:"max-request-bytes"`
	MaxScanLimit            int    `toml:"max-scan-limit"`
	MinScanLimit            int    `toml:"min-scan-limit"`
	IDAllocatorStep         uint   `toml:"id-allocator-step"`

	// Following fields are the settings for the default cluster.
	DefaultClusterName              string `toml:"default-cluster-name"`
	DefaultClusterNodeCount         int    `toml:"default-cluster-node-count"`
	DefaultClusterReplicationFactor int    `toml:"default-cluster-replication-factor"`
	DefaultClusterShardTotal        int    `toml:"default-cluster-shard-total"`

	ClientUrls          string `toml:"client-urls"`
	PeerUrls            string `toml:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls"`

	HTTPPort int `toml:"default-http-port"`
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

	cfg.Logger = "zap"
	cfg.LogOutputs = []string{c.EtcdLog.File}
	cfg.LogLevel = c.EtcdLog.Level

	return cfg, nil
}

// Parser builds the config from the flags.
type Parser struct {
	flagSet        *flag.FlagSet
	cfg            *Config
	configFilePath string
}

func (p *Parser) Parse(arguments []string) (*Config, error) {
	if err := p.flagSet.Parse(arguments); err != nil {
		if err == flag.ErrHelp {
			return nil, ErrHelpRequested.WithCause(err)
		}
		return nil, ErrInvalidCommandArgs.WithCausef("fail to parse flag arguments:%v, err:%v", arguments, err)
	}
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
	defaultNodeName, err := makeDefaultNodeName()
	if err != nil {
		return nil, err
	}
	defaultInitialCluster := makeDefaultInitialCluster(defaultNodeName)

	fs, cfg := flag.NewFlagSet("meta", flag.ContinueOnError), &Config{
		Log: log.Config{
			Level: log.DefaultLogLevel,
			File:  log.DefaultLogFile,
		},
		EtcdLog: log.Config{
			Level: log.DefaultLogLevel,
			File:  log.DefaultLogFile,
		},

		GrpcHandleTimeoutMs: defaultGrpcHandleTimeoutMs,
		EtcdStartTimeoutMs:  defaultEtcdStartTimeoutMs,
		EtcdCallTimeoutMs:   defaultCallTimeoutMs,

		LeaseTTLSec: defaultEtcdLeaseTTLSec,

		NodeName:        defaultNodeName,
		DataDir:         defaultDataDir,
		WalDir:          defaultWalDir,
		StorageRootPath: defaultRootPath,

		InitialCluster:      defaultInitialCluster,
		InitialClusterState: defaultInitialClusterState,
		InitialClusterToken: defaultInitialClusterToken,

		ClientUrls:          defaultClientUrls,
		AdvertiseClientUrls: defaultClientUrls,
		PeerUrls:            defaultPeerUrls,
		AdvertisePeerUrls:   defaultPeerUrls,

		TickIntervalMs:    defaultTickIntervalMs,
		ElectionTimeoutMs: defaultElectionTimeoutMs,

		QuotaBackendBytes:       defaultQuotaBackendBytes,
		AutoCompactionMode:      defaultCompactionMode,
		AutoCompactionRetention: defaultAutoCompactionRetention,
		MaxRequestBytes:         defaultMaxRequestBytes,
		MaxScanLimit:            defaultMaxScanLimit,
		MinScanLimit:            defaultMinScanLimit,
		IDAllocatorStep:         defaultIDAllocatorStep,

		DefaultClusterName:              defaultClusterName,
		DefaultClusterNodeCount:         defaultClusterNodeCount,
		DefaultClusterReplicationFactor: defaultClusterReplicationFactor,
		DefaultClusterShardTotal:        defaultClusterShardTotal,

		HTTPPort: defaultHTTPPort,
	}
	builder := &Parser{
		flagSet: fs,
		cfg:     cfg,
	}

	fs.StringVar(&builder.configFilePath, "config", "", "config file path")

	return builder, nil
}

// ParseConfigFromToml read configuration from the toml file, if the config item already exists, it will be overwritten.
func (p *Parser) ParseConfigFromToml() error {
	if len(p.configFilePath) == 0 {
		log.Info("no config file specified, skip parse config")
		return nil
	}
	log.Info("get config from toml", zap.String("configFile", p.configFilePath))

	file, err := os.ReadFile(p.configFilePath)
	if err != nil {
		log.Error("err", zap.Error(err))
		return errors.WithMessage(err, fmt.Sprintf("read config file, configFile:%s", p.configFilePath))
	}
	log.Info("toml config value", zap.String("config", string(file)))

	err = toml.Unmarshal(file, p.cfg)
	if err != nil {
		log.Error("err", zap.Error(err))
		return errors.WithMessagef(err, "unmarshal toml config, configFile:%s", p.configFilePath)
	}

	return nil
}

func (p *Parser) ParseConfigFromEnvVariables() error {
	var resultErr error
	p.flagSet.VisitAll(func(f *flag.Flag) {
		envVar, exists := os.LookupEnv(f.Name)
		if exists {
			err := f.Value.Set(envVar)
			if err != nil {
				log.Error("set env variable failed", zap.Error(err), zap.String("name", f.Name), zap.String("value", envVar))
				resultErr = err
				return
			}
			log.Info("set env variable", zap.String("name", f.Name), zap.String("value", envVar))
		}
	})
	return resultErr
}
