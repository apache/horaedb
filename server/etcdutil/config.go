// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import (
	"fmt"
	"net/url"
	"os"

	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/server/v3/embed"
)

// NewTestSingleConfig is used to create an etcd config for the unit test purpose.
func NewTestSingleConfig() *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = os.MkdirTemp("/tmp", "test_etcd")
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, _ := url.Parse(tempurl.Alloc())
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg
}

// CleanConfig is used to clean the etcd data for the unit test purpose.
func CleanConfig(cfg *embed.Config) {
	// Clean data directory
	os.RemoveAll(cfg.Dir)
}
