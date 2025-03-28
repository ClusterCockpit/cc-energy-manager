// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"
	"slices"
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
	aggregator        aggregator.Aggregator
	targetToOptimizer map[aggregator.Target]Optimizer
	targetToDevices   map[aggregator.Target][]aggregator.Target
	optimizeTicker    *time.Ticker
	started           bool
	cfg               optimizerConfig
	job               ccspecs.BaseJob
	deviceType        string
	warmUpIterCount   int
	warmUpDone        bool
}

type Optimizer interface {
	Start(float64) (float64, bool)
	Update(float64) float64
	IsConverged() bool
}

func NewJobManager(deviceType string, job ccspecs.BaseJob, rawCfg json.RawMessage) (*JobManager, error) {
	var cfg optimizerConfig

	err := json.Unmarshal(rawCfg, &cfg)
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
		aggregator:        aggregator.New(cfg.AggCfg),
		job:               job,
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
		err = j.initScopeJob(rawCfg)
	case "node":
		/* Calculate local optimum for each individual node of a job and apply it to all the devices of a node */
		err = j.initScopeNode(rawCfg)
	case "device":
		/* Calculate optimum individually for each device for each individual node. */
		err = j.initScopeDevice(rawCfg)
	default:
		cclog.Fatalf("Requested unsupported scope: %s", cfg.Scope)
	}

	if err != nil {
		return nil, err
	}

	cclog.Debugf("Created new job (cluster=%s deviceType=%s)", job.Cluster, deviceType)

	return &j, nil
}

func (j *JobManager) initScopeJob(rawCfg json.RawMessage) error {
	var err error
	target := aggregator.JobScopeTarget()
	j.targetToOptimizer[target], err = NewGssOptimizer(rawCfg)
	if err != nil {
		return err
	}

	devices := make([]aggregator.Target, 0)

	for _, resource := range j.job.Resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.job.Cluster, j.deviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}
	}

	j.targetToDevices[target] = devices
	return nil
}

func (j *JobManager) initScopeNode(rawCfg json.RawMessage) error {
	var err error
	for _, resource := range j.job.Resources {
		/* Create one optimzer for each host */
		target := aggregator.NodeScopeTarget(resource.Hostname)
		j.targetToOptimizer[target], err = NewGssOptimizer(rawCfg)
		if err != nil {
			return err
		}

		devices := make([]aggregator.Target, 0)

		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.job.Cluster, j.deviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}

		j.targetToDevices[target] = devices
	}
	return nil
}

func (j *JobManager) initScopeDevice(rawCfg json.RawMessage) error {
	var err error
	for _, resource := range j.job.Resources {
		for _, deviceId := range controller.Instance.GetDeviceIdsForResources(j.job.Cluster, j.deviceType, resource) {
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

func (j *JobManager) Close() {
	if !j.started {
		j.Debug("Not started, thus not closing")
		return
	}

	j.Debug("Stopping JobManager...")
	j.done <- struct{}{}
	j.wg.Wait()
	j.Debug("Stopped JobManager!")
}

func (j *JobManager) Start() {
	j.wg.Add(1)

	// Enable the ticker, which repeadetly notifies us to run the optimizer.
	// We initially set the interval to something very low, to avoid startup delay.
	j.optimizeTicker = time.NewTicker(time.Duration(1) * time.Second)
	j.started = true

	j.Debug("Starting")

	go func() {
		j.warmUpDone = false
		j.warmUpIterCount = 0

		for {
			select {
			case <-j.done:
				j.optimizeTicker.Stop()
				j.wg.Done()
				return
			case inputVal := <-j.Input:
				if !j.ManagesDeviceOfMetric(inputVal) {
					// The metrics we receive may belong to one of our jobmanager's host,
					// but the actual devices may not be managed by us.
					break
				}
				j.aggregator.AggregateMetric(inputVal)
			case <-j.optimizeTicker.C:
				edpPerTarget := j.aggregator.GetEdpPerTarget()
				if !j.warmUpDone {
					j.UpdateWarmup(edpPerTarget)
				} else {
					j.UpdateNormal(edpPerTarget)
				}
			}
		}
	}()
}

func (j *JobManager) UpdateWarmup(edpPerTarget map[aggregator.Target]float64) {
	// TODO IMPORTANT: Afaik there is currently no logic which checks, whether we have
	// received any new messages or not. If something is misconfigured we will happily optimize
	// on garbage/out-of-date data. This is undesirable behavior.
	j.Debug("Warming up...")
	j.warmUpDone = true

	for target, optimizer := range j.targetToOptimizer {
		edp, ok := edpPerTarget[target]
		if !ok {
			if j.warmUpIterCount >= 10 {
				cclog.Errorf("Unable to warmup. We didn't receive power and performance metrics for 10 iterations. Make sure the configured metrics are available.")
			}
			j.warmUpDone = false
			continue
		}

		optimum, warmUpDoneTarget := optimizer.Start(edp)
		if !warmUpDoneTarget {
			// If just a single optimizer is not warmed up, don't go over to normal operation.
			j.warmUpDone = false
		}
		optimumStr := fmt.Sprintf("%f", optimum)

		for _, device := range j.targetToDevices[target] {
			controller.Instance.Set(j.job.Cluster, device.HostName, j.deviceType, device.DeviceId, j.cfg.ControlName, optimumStr)
		}
	}

	if !j.warmUpDone {
		// Wait until the next tick to run the warmup again
		j.warmUpIterCount++
		j.optimizeTicker.Reset(j.intervalSearch)
		j.Debug("Not ready yet... (iteration=%d)", j.warmUpIterCount)
		return
	}

	j.optimizeTicker.Reset(j.intervalConverged)
	j.Debug("Warmup done! Took %d iterations.", j.warmUpIterCount)
}

func (j *JobManager) UpdateNormal(edpPerTarget map[aggregator.Target]float64) {
	for target, optimizer := range j.targetToOptimizer {
		optimum := fmt.Sprintf("%f", optimizer.Update(edpPerTarget[target]))

		for _, device := range j.targetToDevices[target] {
			controller.Instance.Set(j.job.Cluster, device.HostName, j.deviceType, device.DeviceId, j.cfg.ControlName, optimum)
		}
	}
}

func (j *JobManager) ManagesDeviceOfMetric(m lp.CCMessage) bool {
	// Since multiple jobs may run on one host, we have to detect if the message actually
	// belongs to the device, that our JobManager manages.
	metricHost, _ := m.GetTag("hostname")
	deviceType, ok := m.GetTag("type")
	if !ok {
		j.Debug("Received metric without 'type' tag: %s", m)
		return false
	}

	if deviceType != j.deviceType {
		// Metric device type doesn't belong to the device type that we want to optimizer for
		return false
	}

	// TODO, the following line breaks for node level metrics, because they do not have a deviceId
	// For now that's not a problem, since we only optimize for cpus and gpus, which do not have this problem...
	deviceId, ok := m.GetTag("type-id")
	if !ok {
		j.Debug("Received metric without 'type-id' tag: %s", m)
		return false
	}

	for _, r := range j.job.Resources {
		if r.Hostname == metricHost {
			deviceIds := controller.Instance.GetDeviceIdsForResources(j.job.Cluster, deviceType, r)

			// If the metric's deviceId is present in the list of devices associated with this job manager,
			// accept the message. If not, discard the message
			return slices.Index(deviceIds, deviceId) >= 0
		}
	}

	// This codepath should usually not be reached, since we catch it earlier in the ClusterManager
	// if the metric doesn't belong to one of ours hosts.
	j.Debug("Received metric which doesn't belong to us.")
	return false
}

func (j *JobManager) Debug(fmtstr string, args ...any) {
	subCluster := j.job.SubCluster
	if subCluster == "" {
		// This makes the messages a bit less confusing when SubCluster is blank
		subCluster = "<empty>"
	}
	component := fmt.Sprintf("JobManager(%s,%s,%s,%d)", j.job.Cluster, subCluster, j.deviceType, j.job.JobID)
	msg := fmt.Sprintf(fmtstr, args...)
	cclog.ComponentDebug(component, msg)
}
