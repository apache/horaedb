// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/CeresDB/ceresmeta/server"
	"github.com/CeresDB/ceresmeta/server/config"
)

func main() {
	cfgParser, err := config.MakeConfigParser()
	if err != nil {
		log.Fatalf("fail to generate config builder, err:%v", err)
	}

	cfg, err := cfgParser.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("fail to parse config from command line params, err:%v", err)
	}

	if err := cfg.ValidateAndAdjust(); err != nil {
		log.Fatalf("invalid config, err:%v", err)
	}

	// TODO: Do adjustment to config for preparing joining existing cluster.

	ctx, cancel := context.WithCancel(context.Background())
	srv, err := server.CreateServer(ctx, cfg)
	if err != nil {
		log.Fatalf("fail to create server, err:%v", err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := srv.Run(); err != nil {
		log.Fatalf("fail to run server, err:%v", err)
	}

	<-ctx.Done()
	log.Printf("got signal to exit, signal:%v\n", sig)

	srv.Close()
}
