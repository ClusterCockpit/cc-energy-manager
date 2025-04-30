// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"
	"fmt"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	"github.com/ClusterCockpit/cc-lib/util"
)

type MedianAggregatorConfig struct {
	DeviceType        string `json:"deviceType"`
	PowerMetric       string `json:"powerMetric"`
	PerformanceMetric string `json:"performanceMetric"`
	UseMax            bool   `json:"useMax"`
}

type MedianDeviceState struct {
	powerSamples       []float64
	performanceSamples []float64
	powerReset         bool
	performanceReset   bool
}

type MedianAggregator struct {
	// map[hostname]map[deviceId]MedianDeviceState
	devices            map[string]map[string]*MedianDeviceState
	powerMetric        string
	performanceMetric  string
	deviceType         string
	useMax             bool
}

func NewMedianAggregator(rawConfig json.RawMessage) (*MedianAggregator, error) {
	ag := &MedianAggregator{}
	var config MedianAggregatorConfig

	if err := json.Unmarshal(rawConfig, &config); err != nil {
		cclog.Warnf("Init() > Unmarshal error: %#v", err)
		return nil, err
	}

	if config.PowerMetric == "" || config.PerformanceMetric == "" || config.DeviceType == "" {
		err := fmt.Errorf("Init() : empty metric configuration")
		cclog.Errorf("Init() > config.Path error: %v", err)
		return nil, err
	}

	ag.powerMetric = config.PowerMetric
	ag.performanceMetric = config.PerformanceMetric
	ag.devices = make(map[string]map[string]*MedianDeviceState)
	ag.deviceType = config.DeviceType
	ag.useMax = config.UseMax

	return ag, nil
}

func (a *MedianAggregator) AggregateMetric(m lp.CCMessage) {
	hostname, deviceId, value, ok := checkAndGetMetricFields(m, a.deviceType)
	if !ok {
		return
	}


	var deviceState *MedianDeviceState
	if m.Name() == a.powerMetric || m.Name() == a.performanceMetric {
		devicesOfHostState, ok := a.devices[hostname]
		if !ok {
			devicesOfHostState = make(map[string]*MedianDeviceState)
			a.devices[hostname] = devicesOfHostState
		}

		deviceState, ok = devicesOfHostState[deviceId]
		if !ok {
			deviceState = &MedianDeviceState{
				powerSamples: make([]float64, 0),
				performanceSamples: make([]float64, 0),
			}
			devicesOfHostState[deviceId] = deviceState
		}
	}

	if m.Name() == a.powerMetric {
		// insert measurement into history, discard old values if aggregation was performed since last metric
		if deviceState.powerReset {
			deviceState.powerSamples = make([]float64, 0)
			deviceState.powerReset = false
		}

		deviceState.powerSamples = append(deviceState.powerSamples, value)
	} else if m.Name() == a.performanceMetric {
		// create host specific map if it does not exist yet
		if deviceState.performanceReset {
			deviceState.performanceSamples = make([]float64, 0)
			deviceState.performanceReset = false
		}

		deviceState.performanceSamples = append(deviceState.performanceSamples, value)
	}
}

func (a *MedianAggregator) GetPdpPerTarget() map[Target]float64 {
	// calculate power delay product (PDP) per host and per device
	pdp := make(map[string]map[string]float64)

	for hostname, deviceIdToState := range a.devices {
		pdp[hostname] = make(map[string]float64)

		for deviceId, deviceState := range deviceIdToState {
			if len(deviceState.powerSamples) == 0 && len(deviceState.performanceSamples) == 0 {
				// Initially it may happen that we haven't received a performance sample yet.
				// Ignore, since it will hopefully arrive until the next iteration.
				continue
			}

			// The length of both sample arrays is always > 0, thus Median can't fail.
			powerMedian, _ := util.Median(deviceState.powerSamples)
			performanceMedian, _ := util.Median(deviceState.performanceSamples)
			pdp[hostname][deviceId] = powerMedian / performanceMedian

			deviceState.powerReset = true
			deviceState.performanceReset = true
		}
	}

	return DevicePdpToTargetPdp(pdp, a.useMax)
}
