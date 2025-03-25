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

	cmanager "github.com/ClusterCockpit/cc-energy-manager/internal/clustermanager"
	"github.com/ClusterCockpit/cc-energy-manager/internal/controller"
	cfg "github.com/ClusterCockpit/cc-lib/ccConfig"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-lib/receivers"
	"github.com/ClusterCockpit/cc-lib/sinks"
)

var (
	SinkManager                            sinks.SinkManager
	ReceiveManager                         receivers.ReceiveManager
	ClustManager                           cmanager.ClusterManager
	ShutdownWG                                   sync.WaitGroup
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
func shutdownHandler(shutdownSignal chan os.Signal) {
	// Wait until we receive a UNIX Signal
	<-shutdownSignal

	// Disable handling of further signals. All following signals will unconditionally
	// terminate us. This may be useful if something should go wrong during shutdown handling.
	signal.Stop(shutdownSignal)

	cclog.Info("Shutting down components...")

	if ReceiveManager != nil {
		cclog.Debug("Shutdown ReceiveManager...")
		ReceiveManager.Close()
	}
	if SinkManager != nil {
		cclog.Debug("Shutdown SinkManager...")
		SinkManager.Close()
	}
	if ClustManager != nil {
		cclog.Debug("Shutdown ClusterManager...")
		ClustManager.Close()
	}

	ShutdownWG.Done()
}

func mainFunc() int {
	var err error

	ReadCli()
	cclog.Init(flagLogLevel, flagLogDateTime)
	cfg.Init(flagConfigFile)

	if cfg := cfg.GetPackageConfig("sinks"); cfg != nil {
		SinkManager, err = sinks.New(&ShutdownWG, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Sink configuration must be present")
		return 1
	}

	if cfg := cfg.GetPackageConfig("receivers"); cfg != nil {
		ReceiveManager, err = receivers.New(&ShutdownWG, cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Receiver configuration must be present")
		return 1
	}

	if cfg := cfg.GetPackageConfig("clusters"); cfg != nil {
		ClustManager, err = cmanager.NewClusterManager(cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Optimizer configuration must be present")
		return 1
	}

	if cfg := cfg.GetPackageConfig("controller"); cfg != nil {
		controller.Instance, err = controller.NewCcController(cfg)
		if err != nil {
			cclog.Error(err.Error())
			return 1
		}
	} else {
		cclog.Error("Controller configuration must be present")
		return 1
	}
	defer controller.Instance.Close()

	// Create shutdown handler
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt)
	signal.Notify(shutdownSignal, syscall.SIGTERM)
	ShutdownWG.Add(1)
	go shutdownHandler(shutdownSignal)

	ReceiversToClusterManagerChannel := make(chan lp.CCMessage, 200)
	ClusterManagerToSinksChannel := make(chan lp.CCMessage, 200)

	SinkManager.AddInput(ClusterManagerToSinksChannel)
	ClustManager.AddOutput(ClusterManagerToSinksChannel)
	ReceiveManager.AddOutput(ReceiversToClusterManagerChannel)
	ClustManager.AddInput(ReceiversToClusterManagerChannel)

	SinkManager.Start()
	ReceiveManager.Start()
	ClustManager.Start()

	// Wait that all goroutines finish
	ShutdownWG.Wait()

	return 0
}

func main() {
	exitCode := mainFunc()
	os.Exit(exitCode)
}
