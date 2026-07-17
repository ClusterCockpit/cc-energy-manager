// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ClusterCockpit/cc-energy-manager/internal/aggregator"
	"github.com/ClusterCockpit/cc-energy-manager/internal/controller"
	"github.com/ClusterCockpit/cc-energy-manager/internal/optimizer"

	cclog "github.com/ClusterCockpit/cc-lib/v2/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/v2/ccMessage"
	"github.com/ClusterCockpit/cc-lib/v2/schema"
)

type jobManagerConfig struct {
	Scope               string          `json:"scope"`
	AggCfg              json.RawMessage `json:"aggregator"`
	ControlName         string          `json:"controlName"`
	ControlDefaultValue *float64        `json:"controlDefaultValue"`
	PowerBudgetWeight   float64         `json:"powerBudgetWeight"`
	OptimizerCfg        json.RawMessage `json:"optimizer"`
}

// TODO, why are we using Target, and not TargetTyped, here?

type JobManager struct {
	wg                sync.WaitGroup
	done              chan struct{}
	Input             chan lp.CCMessage
	aggregator        aggregator.Aggregator
	targetToOptimizer map[aggregator.Target]optimizer.Optimizer
	targetToDevices   map[aggregator.Target][]aggregator.Target
	watchdogTicker    *time.Ticker
	watchdogInterval  time.Duration
	watchdogAttempt   int
	started           bool
	stopped           bool
	cfg               jobManagerConfig
	Job               schema.Job
	DeviceType        string
	warmUpDone        bool
	startTime         time.Time
	jobManagerStopped chan<- *JobManager
	ctrl              controller.Controller
}

func NewJobManager(jobManagerStopped chan<- *JobManager, ctrl controller.Controller, deviceType string, job schema.Job, rawCfg json.RawMessage) (*JobManager, error) {
	var cfg jobManagerConfig

	err := json.Unmarshal(rawCfg, &cfg)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}

	if cfg.PowerBudgetWeight <= 0.0 {
		cfg.PowerBudgetWeight = 1.0
	}

	j := JobManager{
		done:              make(chan struct{}, 2), // allow two simultaneous 'done' signals (external, internal)
		started:           false,
		targetToOptimizer: make(map[aggregator.Target]optimizer.Optimizer),
		targetToDevices:   make(map[aggregator.Target][]aggregator.Target),
		cfg:               cfg,
		Job:               job,
		DeviceType:        deviceType,
		jobManagerStopped: jobManagerStopped,
		ctrl:              ctrl,
	}

	/* The functions below initialize j.targetToOptimizer and j.targetToDevices */
	switch cfg.Scope {
	case "job":
		/* Calculate global optimum for all devices on all nodes belonging to job.
		 * Use one optimizer for everything. */
		err = j.initScopeJob(cfg.OptimizerCfg)
	case "node":
		/* Calculate local optimum for each individual node of a job and apply it to all the devices of a node */
		err = j.initScopeNode(cfg.OptimizerCfg)
	case "device":
		/* Calculate optimum individually for each device for each individual node. */
		err = j.initScopeDevice(cfg.OptimizerCfg)
	default:
		cclog.Fatalf("Requested unsupported scope: %s", cfg.Scope)
	}

	if err != nil {
		return nil, err
	}

	j.aggregator, err = aggregator.New(cfg.AggCfg, j.allDevices(), j.DeviceType)
	if err != nil {
		return nil, err
	}

	cclog.Debugf("Created new job (cluster=%s deviceType=%s)", job.Cluster, deviceType)

	return &j, nil
}

func (j *JobManager) initScopeJob(rawCfg json.RawMessage) error {
	var err error
	target := aggregator.JobScopeTarget()
	j.targetToOptimizer[target], err = optimizer.NewOptimizer(rawCfg)
	if err != nil {
		return err
	}

	devices := make([]aggregator.Target, 0)

	for _, resource := range j.Job.Resources {
		for _, deviceId := range j.ctrl.GetDeviceIdsForResources(j.Job.Cluster, j.DeviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}
	}

	j.targetToDevices[target] = devices
	return nil
}

func (j *JobManager) initScopeNode(rawCfg json.RawMessage) error {
	var err error
	for _, resource := range j.Job.Resources {
		/* Create one optimzer for each host */
		target := aggregator.NodeScopeTarget(resource.Hostname)
		j.targetToOptimizer[target], err = optimizer.NewOptimizer(rawCfg)
		if err != nil {
			return err
		}

		devices := make([]aggregator.Target, 0)

		for _, deviceId := range j.ctrl.GetDeviceIdsForResources(j.Job.Cluster, j.DeviceType, resource) {
			devices = append(devices, aggregator.DeviceScopeTarget(resource.Hostname, deviceId))
		}

		j.targetToDevices[target] = devices
	}
	return nil
}

func (j *JobManager) initScopeDevice(rawCfg json.RawMessage) error {
	var err error
	for _, resource := range j.Job.Resources {
		for _, deviceId := range j.ctrl.GetDeviceIdsForResources(j.Job.Cluster, j.DeviceType, resource) {
			/* Create one optimizer for each device on a host to optimize. */
			target := aggregator.DeviceScopeTarget(resource.Hostname, deviceId)
			j.targetToOptimizer[target], err = optimizer.NewOptimizer(rawCfg)
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

func (j *JobManager) allDevices() []aggregator.Target {
	devices := make([]aggregator.Target, 0)

	for _, deviceList := range j.targetToDevices {
		for _, device := range deviceList {
			devices = append(devices, device)
		}
	}

	return devices
}

func (j *JobManager) AddInput(input chan lp.CCMessage) {
	j.Input = input
}

func (j *JobManager) Close() {
	if !j.started {
		j.Debug("Not started, thus not closing")
		return
	}

	if j.stopped {
		// Defacto same condition as above, but don't write to the log, since this
		// is an expected case when a job times out.
		return
	}

	j.Debug("Stopping JobManager...")
	j.done <- struct{}{}
	j.wg.Wait()
	j.Debug("Stopped JobManager!")
}

func (j *JobManager) Start() {
	j.wg.Add(1)

	j.watchdogAttempt = 0
	j.watchdogInterval = time.Duration(300) * time.Second
	j.watchdogTicker = time.NewTicker(j.watchdogInterval)
	j.started = true

	j.Debug("Starting")

	go func() {
		j.warmUpDone = false

		// Unfortunately we don't know the true start time of the job, so we have to manually
		// measure the start time. Though, in practice they shouldn't differ by much.
		// We only use this to cleanup old jobs, where the stop event was missed.
		// So it's not really tragic that there is an inaccuracy.
		j.startTime = time.Now()

		for {
			select {
			case <-j.done:
				j.watchdogTicker.Stop()
				j.ResetToDefault()
				j.wg.Done()
				j.stopped = true

				// Tell the ClusterManager which JobManager (us) terminated, to that it can
				// be removed from the internal list
				j.jobManagerStopped <- j
				return
			case inputVal := <-j.Input:
				if !j.ManagesDeviceOfMetric(inputVal) {
					// The metrics we receive may belong to one of our jobmanager's host,
					// but the actual devices may not be managed by us.
					break
				}
				j.aggregator.MetricAdd(inputVal)
				if j.aggregator.MetricsReady() {
					j.Update(j.aggregator.GetEdpPerTarget())
					j.aggregator.MetricsReset()
					j.watchdogTicker.Reset(j.watchdogInterval)

					maxDuration := time.Duration(j.Job.Walltime) * time.Second
					if j.Job.Walltime > 0 && j.startTime.Add(maxDuration).Before(time.Now()) {
						j.Debug("Job exceeded maximum walltime (%v). Stopping job manager...", j.Job.Walltime)
						j.done <- struct{}{}
					}
				}
			case <-j.watchdogTicker.C:
				j.Debug("Watchdog timer triggered. Did we receive any metrics? Make sure the configuration is correct")

				if j.watchdogAttempt >= 3 {
					j.Debug("Maximum number of watchdog timeouts triggered. Terminating job manager...")
					j.done <- struct{}{}
				}
				j.watchdogAttempt++
			}
		}
	}()
}

func (j *JobManager) Update(edpPerTarget map[aggregator.Target]float64) {
	for target, optimizer := range j.targetToOptimizer {
		edp := edpPerTarget[target]

		optimum := fmt.Sprintf("%f", optimizer.Update(edp))
		j.Debug("%v: edp=%f --> optimum=%s", target, edp, optimum)

		for _, device := range j.targetToDevices[target] {
			j.ctrl.Set(j.Job.Cluster, string(device.HostName), j.DeviceType, string(device.DeviceId), j.cfg.ControlName, optimum)
		}
	}
}

func (j *JobManager) ResetToDefault() {
	if j.cfg.ControlDefaultValue == nil {
		j.Debug("No control default value specified in config. Not resetting power limits.")
		return
	}

	v := fmt.Sprintf("%f", *j.cfg.ControlDefaultValue)

	j.Debug("Resetting device to default value...")
	for target := range j.targetToOptimizer {
		for _, device := range j.targetToDevices[target] {
			j.ctrl.Set(j.Job.Cluster, string(device.HostName), j.DeviceType, string(device.DeviceId), j.cfg.ControlName, v)
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

	if deviceType != j.DeviceType {
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

	for _, r := range j.Job.Resources {
		if r.Hostname == metricHost {
			deviceIds := j.ctrl.GetDeviceIdsForResources(j.Job.Cluster, deviceType, r)

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

func (j *JobManager) PowerBudgetWeight() float64 {
	return j.cfg.PowerBudgetWeight * float64(j.deviceCount())
}

func (j *JobManager) PowerBudgetSet(power float64) {
	if len(j.targetToOptimizer) <= 0 || j.deviceCount() <= 0 {
		j.Debug("Cannot set PowerBudget: No optimizers to account for")
		return
	}

	power /= float64(j.deviceCount())

	for _, optimizer := range j.targetToOptimizer {
		powerBudgetLowerCfg, powerBudgetUpperCfg := optimizer.GetBordersCfg()
		powerBudgetLowerCur, _ := optimizer.GetBordersCur()

		powerBudgetUpperCur := min(power, powerBudgetUpperCfg)
		if powerBudgetUpperCur <= powerBudgetLowerCur {
			if powerBudgetUpperCur > powerBudgetLowerCfg {
				powerBudgetLowerCur = powerBudgetLowerCfg
			} else {
				j.Debug("Cannot set powerlimit %vW [%vW, %vW] below minimum allowed [%vW, %vW]",
					power, powerBudgetLowerCur, powerBudgetUpperCur, powerBudgetLowerCfg, powerBudgetUpperCfg)
				continue
			}
		}

		optimizer.SetBorders(powerBudgetLowerCur, powerBudgetUpperCur)
	}
}

func (j *JobManager) deviceCount() int {
	count := 0
	for _, devices := range j.targetToDevices {
		count += len(devices)
	}
	return count
}

func (j *JobManager) Debug(fmtstr string, args ...any) {
	subCluster := j.Job.SubCluster
	if subCluster == "" {
		// This makes the messages a bit less confusing when SubCluster is blank
		subCluster = "<empty>"
	}
	component := fmt.Sprintf("JobManager(%s,%s,%s,%d)", j.Job.Cluster, subCluster, j.DeviceType, j.Job.JobID)
	msg := fmt.Sprintf(fmtstr, args...)
	cclog.ComponentDebug(component, msg)
}
