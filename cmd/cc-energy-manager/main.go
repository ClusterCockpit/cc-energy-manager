// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	cmanager "github.com/ClusterCockpit/cc-energy-manager/pkg/ClusterManager"
	"github.com/ClusterCockpit/cc-lib/receivers"
	"github.com/ClusterCockpit/cc-lib/sinks"

	// DB added
	opt "github.com/ClusterCockpit/cc-energy-manager/pkg/Optimizer"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type CentralConfigFile struct {
	Interval            string `json:"interval"`
	SinkConfigFile      string `json:"sinks"`
	ReceiverConfigFile  string `json:"receivers"`
	OptimizerConfigFile string `json:"optimizer"`
}

func LoadCentralConfiguration(file string, config *CentralConfigFile) error {
	configFile, err := os.Open(file)
	if err != nil {
		cclog.Error(err.Error())
		return err
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(config)
	return err
}

type RuntimeConfig struct {
	Interval   time.Duration
	CliArgs    map[string]string
	ConfigFile CentralConfigFile

	SinkManager    sinks.SinkManager
	ReceiveManager receivers.ReceiveManager
	// Router         router.MetricRouter
	ClustManager cmanager.ClusterManager
	// DB
	Optimizer opt.Optimizer

	Channels []chan lp.CCMetric
	Sync     sync.WaitGroup
}

func ReadCli() map[string]string {
	var m map[string]string
	cfg := flag.String("config", "./config.json", "Path to configuration file")
	logfile := flag.String("log", "stderr", "Path for logfile")
	once := flag.Bool("once", false, "Run all collectors only once")
	debug := flag.Bool("debug", false, "Activate debug output")
	flag.Parse()
	m = make(map[string]string)
	m["configfile"] = *cfg
	m["logfile"] = *logfile
	if *once {
		m["once"] = "true"
	} else {
		m["once"] = "false"
	}
	if *debug {
		m["debug"] = "true"
		cclog.SetDebug()
	} else {
		m["debug"] = "false"
	}
	return m
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
	// if config.Router != nil {
	// 	cclog.Debug("Shutdown Router...")
	// 	config.Router.Close()
	// }
	if config.ClustManager != nil {
		cclog.Debug("Shutdown ClusterManager...")
		config.ClustManager.Close()
	}
	// if config.Optimizer != nil {
	// 	cclog.Debug("Shutdown Optimizer....")
	// 	config.Optimizer.Close()
	// }
}

func mainFunc() int {
	var err error

	// Initialize runtime configuration
	rcfg := RuntimeConfig{
		SinkManager:    nil,
		ReceiveManager: nil,
		// Router:         nil,
		// DB added an Optimer to rcfg
		// Optimizer:    nil,
		ClustManager: nil,
		CliArgs:      ReadCli(),
	}

	// Load and check configuration
	err = LoadCentralConfiguration(rcfg.CliArgs["configfile"], &rcfg.ConfigFile)
	if err != nil {
		cclog.Error("Error reading configuration file ", rcfg.CliArgs["configfile"], ": ", err.Error())
		return 1
	}

	// Properly use duration parser with inputs like '60s', '5m' or similar
	if len(rcfg.ConfigFile.Interval) > 0 {
		t, err := time.ParseDuration(rcfg.ConfigFile.Interval)
		if err != nil {
			cclog.Error("Configuration value 'interval' no valid duration")
		}
		rcfg.Interval = t
		if rcfg.Interval == 0 {
			cclog.Error("Configuration value 'interval' must be greater than zero")
			return 1
		}
	}

	if len(rcfg.ConfigFile.SinkConfigFile) == 0 {
		cclog.Error("Sink configuration file must be set")
		return 1
	}
	if len(rcfg.ConfigFile.ReceiverConfigFile) == 0 {
		cclog.Error("Receivers configuration file must be set")
		return 1
	}
	if len(rcfg.ConfigFile.OptimizerConfigFile) == 0 {
		cclog.Error("Optimizer configuration file must be set")
		return 1
	}

	// Set log file
	if logfile := rcfg.CliArgs["logfile"]; logfile != "stderr" {
		cclog.SetOutput(logfile)
	}

	// Create new sink
	rcfg.SinkManager, err = sinks.New(&rcfg.Sync, rcfg.ConfigFile.SinkConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return 1
	}

	// Create new receive manager
	rcfg.ReceiveManager, err = receivers.New(&rcfg.Sync, rcfg.ConfigFile.ReceiverConfigFile)
	if err != nil {
		cclog.Error(err.Error())
		return 1
	}

	rcfg.ClustManager, err = cmanager.NewClusterManager(&rcfg.Sync, rcfg.ConfigFile.OptimizerConfigFile)
	if err != nil {
		cclog.Error(err.Error())
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
