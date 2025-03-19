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
	WindowSize        int    `json:"windowSize"`
}

type MedianAggregator struct {
	// map[hostname]map[deviceId][]sample
	powerSamples       map[string]map[string][]float64
	performanceSamples map[string]map[string][]float64
	powerMetric        string
	performanceMetric  string
	deviceType         string
	windowSize         int
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

	if config.WindowSize <= 0 {
		cclog.Errorf("MedianAggregator: Window size must be >= 1: Adjusting to 1")
		config.WindowSize = 1
	}

	ag.powerMetric = config.PowerMetric
	ag.performanceMetric = config.PerformanceMetric
	ag.powerSamples = make(map[string]map[string][]float64)
	ag.performanceSamples = make(map[string]map[string][]float64)
	ag.deviceType = config.DeviceType
	ag.windowSize = config.WindowSize

	return ag, nil
}

func (a *MedianAggregator) AggregateMetric(m lp.CCMessage) {
	hostname, deviceId, value, ok := checkAndGetMetricFields(m, a.deviceType)
	if !ok {
		return
	}

	if m.Name() == a.powerMetric {
		// create host specific map if it does not exist yet
		deviceIdToPowerSamples, ok := a.powerSamples[hostname]
		if !ok {
			deviceIdToPowerSamples = make(map[string][]float64)
			a.powerSamples[hostname] = deviceIdToPowerSamples
		}

		// create device specific window
		if _, ok := deviceIdToPowerSamples[deviceId]; !ok {
			deviceIdToPowerSamples[deviceId] = make([]float64, 0)
		}

		// insert measurement into history, discard old values if window is to big
		l := len(deviceIdToPowerSamples[deviceId])
		if l >= a.windowSize {
			deviceIdToPowerSamples[deviceId] = append(deviceIdToPowerSamples[deviceId][:l-1], value)
		} else {
			deviceIdToPowerSamples[deviceId] = append(deviceIdToPowerSamples[deviceId], value)
		}
	} else if m.Name() == a.performanceMetric {
		// create host specific map if it does not exist yet
		deviceIdToPerformanceSamples, ok := a.performanceSamples[hostname]
		if !ok {
			deviceIdToPerformanceSamples = make(map[string][]float64)
			a.performanceSamples[hostname] = deviceIdToPerformanceSamples
		}

		// create device specific window
		if _, ok := deviceIdToPerformanceSamples[deviceId]; !ok {
			deviceIdToPerformanceSamples[deviceId] = make([]float64, 0)
		}

		// insert measurement into history, discard old values if window is to big
		l := len(deviceIdToPerformanceSamples[deviceId])
		if l >= a.windowSize {
			deviceIdToPerformanceSamples[deviceId] = append(deviceIdToPerformanceSamples[deviceId][:l-1], value)
		} else {
			deviceIdToPerformanceSamples[deviceId] = append(deviceIdToPerformanceSamples[deviceId], value)
		}
	}
}

func (a *MedianAggregator) GetEdpPerTarget() map[Target]float64 {
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

		for deviceId, powerSampleWindow := range deviceIdToPowerSamples {
			performanceSampleWindow, ok := deviceIdToPerformanceSamples[deviceId]
			if !ok {
				// same here
				continue
			}

			// When the window is created, it is always filled with the first value,
			// thus the window is never empty and Median won't fail.
			powerMedian, _ := util.Median(powerSampleWindow)
			performanceMedian, _ := util.Median(performanceSampleWindow)
			edp[hostname][deviceId] = powerMedian / performanceMedian
		}
	}

	return DeviceEdpToTargetEdp(edp)
}
