// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClusterCockpit/cc-energy-manager/internal/aggregator"
	"github.com/ClusterCockpit/cc-energy-manager/internal/controller"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type optimizerConfig struct {
	Scope             string          `json:"scope"`
	AggCfg            json.RawMessage `json:"aggregator"`
	ControlName       string          `json:"controlName"`
	IntervalConverged string          `json:"intervalConverged"`
	IntervalSearch    string          `json:"intervalSearch"`
}

type JobManager struct {
	wg                sync.WaitGroup
	done              chan struct{}
	Input             chan lp.CCMessage
	intervalSearch    time.Duration
	intervalConverged time.Duration
	cluster           string
	resources         []*ccspecs.Resource
	aggregator        aggregator.Aggregator
	targetToOptimizer map[aggregator.Target]Optimizer
	targetToDevices   map[aggregator.Target][]aggregator.Target
	optimizeTicker    *time.Ticker
	started           bool
	cfg               optimizerConfig
	deviceType        string
}

type Optimizer interface {
	Start(float64) (int, bool)
	Update(float64) int
	IsConverged() bool
}

func NewJobManager(cluster string, deviceType string, resources []*ccspecs.Resource,
	config json.RawMessage,
) (*JobManager, error) {
	var cfg optimizerConfig

	err := json.Unmarshal(config, &cfg)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}

	j := JobManager{
		done:              make(chan struct{}),
		started:           false,
		targetToOptimizer: make(map[aggregator.Target]Optimizer),
		targetToDevices:   make(map[aggregator.Target][]aggregator.Target),
		cfg:               cfg,
		resources:         resources,
		aggregator:        aggregator.New(cfg.AggCfg),
		cluster:           cluster,
		deviceType:        deviceType,
	}

	intervalSearch, err := time.ParseDuration(cfg.IntervalSearch)
	if err != nil {
		err := fmt.Errorf("failed to parse search interval %s: %v", cfg.IntervalSearch, err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}
	j.intervalSearch = intervalSearch

	intervalConverged, err := time.ParseDuration(cfg.IntervalConverged)
	if err != nil {
		err := fmt.Errorf("failed to parse converged interval %s: %v", cfg.IntervalConverged, err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}
	j.intervalConverged = intervalConverged

	/* The functions below initialize j.targetToOptimizer and j.targetToDevices */
	switch cfg.Scope {
	case "job":
		/* Calculate global optimum for all devices on all nodes belonging to job.
		 * Use one optimizer for everything. */
		err = initScopeJob(&j, resources, cfg, config)
	case "node":
		/* Calculate local optimum for each individual node of a job and apply it to all the devices of a node */
		err = initScopeNode(&j, resources, cfg, config)
	case "device":
		/* Calculate optimum individually for each device for each individual node. */
		err = initScopeDevice(&j, resources, cfg, config)
	default:
		cclog.Fatalf("Requested unsupported scope: %s", cfg.Scope)
	}

	if err != nil {
		return nil, err
	}

	cclog.Debugf("Created new job (cluster=%s deviceType=%s)", cluster, deviceType)

	return &j, nil
}

func initScopeJob(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	target := aggregator.JobScopeTarget()
	j.targetToOptimizer[target], err = NewGssOptimizer(rawCfg)
	if err != nil {
		return err
	}

	devices := make([]aggregator.Target, 0)

	for _, resource := range resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, j.deviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}
	}

	j.targetToDevices[target] = devices
	return nil
}

func initScopeNode(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	for _, resource := range resources {
		/* Create one optimzer for each host */
		target := aggregator.NodeScopeTarget(resource.Hostname)
		j.targetToOptimizer[target], err = NewGssOptimizer(rawCfg)
		if err != nil {
			return err
		}

		devices := make([]aggregator.Target, 0)

		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, j.deviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}

		j.targetToDevices[target] = devices
	}
	return nil
}

func initScopeDevice(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	for _, resource := range resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, j.deviceType, resource) {
			/* Create one optimizer for each device on a host to optimize. */
			target := aggregator.DeviceScopeTarget(resource.Hostname, deviceId)
			j.targetToOptimizer[target], err = NewGssOptimizer(rawCfg)
			if err != nil {
				return err
			}
			/* In "device" scope, `target` and `device` are indentical */
			device := target
			j.targetToDevices[target] = []aggregator.Target{device}
		}
	}
	return nil
}

func isSocketMetric(metric string) bool {
	return (strings.Contains(metric, "power") || strings.Contains(metric, "energy") || metric == "mem_bw")
}

func isAcceleratorMetric(metric string) bool {
	return strings.HasPrefix(metric, "acc_")
}

func (j *JobManager) AddInput(input chan lp.CCMessage) {
	j.Input = input
}

func (r *JobManager) Close() {
	if !r.started {
		cclog.ComponentDebug("JobManager", "Not started, thus not closing")
		return
	}

	cclog.ComponentDebug("JobManager", "Stopping JobManager...")
	r.done <- struct{}{}
	r.wg.Wait()
	cclog.ComponentDebug("JobManager", "Stopped JobManager!")
}

func (j *JobManager) Start() {
	j.wg.Add(1)

	// Enable the ticker, which repeadetly notifies us to run the optimizer.
	// We initially set the interval to something very low, to avoid startup delay.
	j.optimizeTicker = time.NewTicker(time.Duration(1) * time.Second)
	j.started = true

	cclog.ComponentDebug("JobManager", "Starting")

	go func() {
		warmUpDone := false
		warmUpIterCount := 0
		for {
			select {
			case <-j.done:
				j.optimizeTicker.Stop()
				j.wg.Done()
				return
			case inputVal := <-j.Input:
				j.aggregator.AggregateMetric(inputVal)
			case <-j.optimizeTicker.C:
				edpPerTarget := j.aggregator.GetEdpPerTarget()

				if !warmUpDone {
					cclog.ComponentDebug("JobManager", "Warming up...")
					j.optimizeTicker.Reset(j.intervalSearch)
					warmUpDone = true
					for target, optimizer := range j.targetToOptimizer {
						edp, ok := edpPerTarget[target]
						if !ok {
							// initially 
							warmUpDone = false
							continue
						}

						if _, warmUpDoneNew := optimizer.Start(edp); !warmUpDoneNew {
							// If just a single optimizer is not warmed up, don't go over to normal operation.
							warmUpDone = false
						}
					}

					if !warmUpDone {
						// Wait until the next tick to run the warmup again
						warmUpIterCount++
						cclog.ComponentDebug("JobManager", "Not ready yet ...", warmUpIterCount)
						break
					}

					j.optimizeTicker.Reset(j.intervalConverged)
					cclog.ComponentDebug("JobManager", "Warmup done!")
				}

				for target, optimizer := range j.targetToOptimizer {
					optimum := fmt.Sprintf("%d", optimizer.Update(edpPerTarget[target]))

					for _, device := range j.targetToDevices[target] {
						controller.Instance.Set(j.cluster, device.HostName, j.deviceType, device.DeviceId, j.cfg.ControlName, optimum)
					}
				}
			}
		}
	}()
}
