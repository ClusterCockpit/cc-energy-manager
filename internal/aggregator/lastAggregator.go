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
)

type LastAggregatorConfig struct {
	DeviceType        string `json:"deviceType"`
	PowerMetric       string `json:"powerMetric"`
	PerformanceMetric string `json:"performanceMetric"`
}

type LastAggregator struct {
	// map[hostname]map[deviceId]sample
	powerSamples       map[string]map[string]float64
	performanceSamples map[string]map[string]float64
	powerMetric        string
	performanceMetric  string
	deviceType         string
}

func NewLastAggregator(rawConfig json.RawMessage) (*LastAggregator, error) {
	ag := &LastAggregator{}
	var config LastAggregatorConfig

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
	ag.powerSamples = make(map[string]map[string]float64)
	ag.performanceSamples = make(map[string]map[string]float64)
	ag.deviceType = config.DeviceType

	return ag, nil
}

func (a *LastAggregator) AggregateMetric(m lp.CCMessage) {
	hostname, deviceId, value, ok := checkAndGetMetricFields(m, a.deviceType)
	if !ok {
		return
	}

	if m.Name() == a.powerMetric {
		// create host specific map if it does not exist yet
		deviceIdToPowerSamples, ok := a.powerSamples[hostname]
		if !ok {
			deviceIdToPowerSamples = make(map[string]float64)
			a.powerSamples[hostname] = deviceIdToPowerSamples
		}

		deviceIdToPowerSamples[deviceId] = value
	} else if m.Name() == a.performanceMetric {
		// create host specific map if it does not exist yet
		deviceIdToPerformanceSamples, ok := a.performanceSamples[hostname]
		if !ok {
			deviceIdToPerformanceSamples = make(map[string]float64)
			a.performanceSamples[hostname] = deviceIdToPerformanceSamples
		}

		deviceIdToPerformanceSamples[deviceId] = value
	}
}

func (a *LastAggregator) GetEdpPerTarget() map[Target]float64 {
	// calculate energy delay product (EDP) per host and per device
	edp := make(map[string]map[string]float64)

	for hostname, deviceIdToPowerSamples := range a.powerSamples {
		deviceIdToPerformanceSamples, ok := a.performanceSamples[hostname]
		if !ok {
			// Initially it may happen that we haven't received a performance sample yet.
			// Ignore, since it will hopefully arrive until the next iteration.
			continue
		}

		edp[hostname] = make(map[string]float64)

		for deviceId, powerSample := range deviceIdToPowerSamples {
			performanceSample, ok := deviceIdToPerformanceSamples[deviceId]
			if !ok {
				// same here
				continue
			}

			edp[hostname][deviceId] = powerSample / performanceSample
		}
	}

	return DeviceEdpToTargetEdp(edp)
}
