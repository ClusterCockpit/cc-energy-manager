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

type controlConfig struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type optimizerConfig struct {
	Scope             string          `json:"scope"`
	OptDeviceType     string          `json:"optDeviceType"`
	AggCfg            json.RawMessage `json:"aggregator"`
	ControlCfg        controlConfig   `json:"control"`
	IntervalConverged string          `json:"intervalConverged"`
	IntervalSearch    string          `json:"intervalSearch"`
}

type JobManager struct {
	wg                *sync.WaitGroup
	Done              chan bool
	Input             chan lp.CCMessage
	interval          time.Duration
	cluster           string
	resources         []*ccspecs.Resource
	aggregator        aggregator.Aggregator
	targetToOptimizer map[string]Optimizer
	targetToDevices   map[string][]string
	ticker            time.Ticker
	started           bool
	cfg               optimizerConfig
}

type Optimizer interface {
	Start(float64) (int, bool)
	Update(float64) int
	IsConverged() bool
}

/* A `target` string may be:
 * - 'node01/socket/42'                   (device scope)
 * - 'node01/nvidia_gpu/00000000:00:3f.0' (device scope)
 * - 'node01'                             (node scope)
 * - ''                                   (job scope)
 *
 * A `device` string is a `target` string, which must be at target scope. */
func TargetName(hostname *string, deviceType *string, deviceId *string) string {
	if hostname == nil {
		return ""
	}
	if deviceType != nil && deviceId != nil {
		return fmt.Sprintf("%s/%s/%s", *hostname, *deviceType, *deviceId)
	}
	return *hostname
}

func NewJobManager(wg *sync.WaitGroup, cluster string, resources []*ccspecs.Resource,
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
		wg:                wg,
		Done:              make(chan bool),
		started:           false,
		targetToOptimizer: make(map[string]Optimizer),
		targetToDevices:   make(map[string][]string),
		cfg:               cfg,
		resources:         resources,
		aggregator:        aggregator.New(cfg.AggCfg),
		cluster:           cluster,
	}

	t, err := time.ParseDuration(cfg.IntervalSearch)
	if err != nil {
		err := fmt.Errorf("failed to parse interval %s: %v", cfg.IntervalSearch, err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}

	j.interval = t

	/* Assert a valid device type here, so that we don't have to check edge cases everywhere else. */
	if cfg.OptDeviceType != "socket" && cfg.OptDeviceType != "nvidia_gpu" {
		return nil, fmt.Errorf("invalid device to optimizer power for: %s", cfg.OptDeviceType)
	}

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

	return &j, nil
}

func initScopeJob(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	targetName := TargetName(nil, nil, nil)
	j.targetToOptimizer[targetName], err = NewGssOptimizer(rawCfg)
	if err != nil {
		return err
	}

	devices := make([]string, 0)

	for _, resource := range resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, cfg.OptDeviceType, resource) {
			devices = append(devices, TargetName(&resource.Hostname, &cfg.OptDeviceType, &deviceId))
		}
	}

	j.targetToDevices[targetName] = devices
	return nil
}

func initScopeNode(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	for _, resource := range resources {
		/* Create one optimzer for each host */
		targetName := TargetName(&resource.Hostname, nil, nil)
		j.targetToOptimizer[targetName], err = NewGssOptimizer(rawCfg)
		if err != nil {
			return err
		}

		devices := make([]string, 0)

		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, cfg.OptDeviceType, resource) {
			devices = append(devices, TargetName(&resource.Hostname, &cfg.OptDeviceType, &deviceId))
		}

		j.targetToDevices[targetName] = devices
	}
	return nil
}

func initScopeDevice(j *JobManager, resources []*ccspecs.Resource, cfg optimizerConfig, rawCfg json.RawMessage) error {
	var err error
	for _, resource := range resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.cluster, cfg.OptDeviceType, resource) {
			/* Create one optimizer for each device on a host to optimize. */
			targetName := TargetName(&resource.Hostname, &cfg.OptDeviceType, &deviceId)
			j.targetToOptimizer[targetName], err = NewGssOptimizer(rawCfg)
			if err != nil {
				return err
			}
			/* In "device" scope, `target` and `device` strings are indentical */
			deviceName := targetName
			j.targetToDevices[targetName] = []string{deviceName}
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

// TODO: Fix correct shutdown
func (r *JobManager) Close() {
	if r.started {
		cclog.ComponentDebug("JobManager", "Sending Done")
		r.Done <- true
		<-r.Done
		cclog.ComponentDebug("JobManager", "STOPPING Timer")
		// os.ticker.Stop()
	}
	cclog.ComponentDebug("JobManager", "Waiting for optimizer to exit")
	r.wg.Done()
	cclog.ComponentDebug("JobManager", "CLOSE")
}

func (j *JobManager) Start() {
	j.wg.Add(1)
	// Ticker for running the optimizer
	j.ticker = *time.NewTicker(j.interval)
	j.started = true

	go func(done chan bool, wg *sync.WaitGroup) {
		warmUpDone := false
		for {
			select {
			case <-done:
				wg.Done()
				close(done)
				return
			case inputVal := <-j.Input:
				j.aggregator.Add(inputVal)
			case <-j.ticker.C:
				/* TODO the current spec of the aggregator must be fixed to accomodate our changes.
				 * At the moment the LastAggregator only returns a host as key in the map, which is pretty useless.
				 * We need it to return a target name. See above for details. */
				aggregatedMetrics := j.aggregator.Get()

				for !warmUpDone {
					for targetName, optimizer := range j.targetToOptimizer {
						_, warmUpDone = optimizer.Start(aggregatedMetrics[targetName])
					}
				}

				for target, optimizer := range j.targetToOptimizer {
					optimum := fmt.Sprintf("%d", optimizer.Update(aggregatedMetrics[target]))

					for _, device := range j.targetToDevices[target] {
						/* `device` is a full string like: "node01/nvidia_gpu/00000000:1f.2.0" */
						controller.Instance.Set(j.cluster, device, j.cfg.ControlCfg.Name, optimum)
					}
				}
			}
		}
	}(j.Done, j.wg)
}
