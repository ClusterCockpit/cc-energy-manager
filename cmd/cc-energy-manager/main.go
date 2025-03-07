// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	cmanager "github.com/ClusterCockpit/cc-energy-manager/internal/clustermanager"
	cfg "github.com/ClusterCockpit/cc-lib/ccConfig"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-lib/receivers"
	"github.com/ClusterCockpit/cc-lib/sinks"
)

type RuntimeConfig struct {
	Interval       time.Duration
	SinkManager    sinks.SinkManager
	ReceiveManager receivers.ReceiveManager
	ClustManager   cmanager.ClusterManager
	Channels       []chan lp.CCMessage
	Sync           sync.WaitGroup
}

var (
	flagOnce, flagVersion, flagLogDateTime bool
	flagConfigFile, flagLogLevel           string
)

func ReadCli() {
	flag.StringVar(&flagConfigFile, "config", "./config.json", "Path to configuration file")
	flag.StringVar(&flagLogLevel, "loglevel", "warn", "Sets the logging level: `[debug,info,warn (default),err,fatal,crit]`")
	flag.BoolVar(&flagLogDateTime, "logdate", false, "Set this flag to add date and time to log messages")
	flag.BoolVar(&flagOnce, "once", false, "Run all collectors only once")
	flag.Parse()
}

// General shutdownHandler function that gets executed in case of interrupt or graceful shutdownHandler
func shutdownHandler(config *RuntimeConfig, shutdownSignal chan os.Signal) {
	defer config.Sync.Done()

	<-shutdownSignal
	// Remove shutdown handler
	// every additional interrupt signal will stop without cleaning up
	signal.Stop(shutdownSignal)

	cclog.Info("Shutdown...")

	if config.ReceiveManager != nil {
		cclog.Debug("Shutdown ReceiveManager...")
		config.ReceiveManager.Close()
	}
	if config.SinkManager != nil {
		cclog.Debug("Shutdown SinkManager...")
		config.SinkManager.Close()
	}
	if config.ClustManager != nil {
		cclog.Debug("Shutdown ClusterManager...")
		config.ClustManager.Close()
	}
}

func mainFunc() int {
	var err error

	// Initialize runtime configuration
	rcfg := RuntimeConfig{
		SinkManager:    nil,
		ReceiveManager: nil,
		ClustManager:   nil,
	}

	ReadCli()
	// Initialize ccLogger
	cclog.Init(flagLogLevel, flagLogDateTime)

	// Load configuration
	cfg.Init(flagConfigFile)

	// Create new sink
	if cfg := cfg.GetPackageConfig("sinks"); cfg != nil {
		rcfg.SinkManager, err = sinks.New(&rcfg.Sync, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Sink configuration must be present")
		return 1
	}

	// Create new receive manager
	if cfg := cfg.GetPackageConfig("receivers"); cfg != nil {
		rcfg.ReceiveManager, err = receivers.New(&rcfg.Sync, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Receiver configuration must be present")
		return 1
	}

	if cfg := cfg.GetPackageConfig("optimizer"); cfg != nil {
		rcfg.ClustManager, err = cmanager.NewClusterManager(&rcfg.Sync, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Optimizer configuration must be present")
		return 1
	}

	// Create shutdown handler
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt)
	signal.Notify(shutdownSignal, syscall.SIGTERM)
	rcfg.Sync.Add(1)
	go shutdownHandler(&rcfg, shutdownSignal)

	// RouterToOptimizerChannel := make(chan lp.CCMessage, 200)
	ReceiversToClusterManagerChannel := make(chan lp.CCMessage, 200)
	ClusterManagerToSinksChannel := make(chan lp.CCMessage, 200)

	rcfg.SinkManager.AddInput(ClusterManagerToSinksChannel)
	rcfg.ClustManager.AddOutput(ClusterManagerToSinksChannel)
	rcfg.ReceiveManager.AddOutput(ReceiversToClusterManagerChannel)
	rcfg.ClustManager.AddInput(ReceiversToClusterManagerChannel)

	// Start the managers
	rcfg.SinkManager.Start()
	rcfg.ReceiveManager.Start()
	rcfg.ClustManager.Start()

	// Wait that all goroutines finish
	rcfg.Sync.Wait()

	return 0
}

func main() {
	exitCode := mainFunc()
	os.Exit(exitCode)
}
