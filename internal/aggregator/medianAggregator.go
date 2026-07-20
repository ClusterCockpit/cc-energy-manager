// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"
	"fmt"
	"math"

	cclog "github.com/ClusterCockpit/cc-lib/v2/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/v2/ccMessage"
	"github.com/ClusterCockpit/cc-lib/v2/util"
)

type MedianAggregatorConfig struct {
	BasePower          float64                `json:"basePower"`
	WindowSize         int                    `json:"windowSize"`
	PowerMetrics       map[string]MetricRange `json:"powerMetrics"`
	PerformanceMetrics map[string]MetricRange `json:"performanceMetrics"`
	ReductionMode      string                 `json:"reductionMode"`
}

type MetricStaged struct {
	power       float64
	performance float64
}

type MedianAggregator struct {
	// map[hostname][index]map[deviceId]MetricStaged
	metricsStaged   map[HostNameString][]map[DeviceIdString]MetricStaged
	metricsReady    bool
	devicesToManage map[HostNameString]map[DeviceIdString]struct{}

	powerMetrics       map[MetricNameString]MetricRange
	performanceMetrics map[MetricNameString]MetricRange

	metricsIncoming map[HostNameString]map[DeviceIdString]map[MetricNameString]TargetMetricValue

	windowSize       int
	deviceType       DeviceTypeString
	basePower        float64
	edpReductionMode EdpReductionMode
}

func NewMedianAggregator(rawConfig json.RawMessage, devices []Target, deviceType string) (*MedianAggregator, error) {
	a := &MedianAggregator{}
	var config MedianAggregatorConfig

	err := json.Unmarshal(rawConfig, &config)
	if err != nil {
		cclog.Errorf("MedianAggregator config error: %#v", err)
		return nil, err
	}

	if len(config.PowerMetrics) == 0 {
		return nil, fmt.Errorf("PowerMetrics map must not be empty")
	}

	if len(config.PerformanceMetrics) == 0 {
		return nil, fmt.Errorf("PerformanceMetrics map must not be empty")
	}

	a.devicesToManage = make(map[HostNameString]map[DeviceIdString]struct{})
	for _, device := range devices {
		devicesToManageForHost, ok := a.devicesToManage[device.HostName]
		if !ok {
			devicesToManageForHost = make(map[DeviceIdString]struct{})
			a.devicesToManage[device.HostName] = devicesToManageForHost
		}
		devicesToManageForHost[device.DeviceId] = struct{}{}
	}
	a.MetricsReset()

	a.powerMetrics = make(map[MetricNameString]MetricRange)
	for metricName, metricRange := range config.PowerMetrics {
		a.powerMetrics[MetricNameString(metricName)] = metricRange.ConfigInit()
	}
	a.performanceMetrics = make(map[MetricNameString]MetricRange)
	for metricName, metricRange := range config.PerformanceMetrics {
		a.performanceMetrics[MetricNameString(metricName)] = metricRange.ConfigInit()
	}

	a.deviceType = DeviceTypeString(deviceType)
	a.basePower = config.BasePower
	if a.basePower < 0.0 {
		return nil, fmt.Errorf("basePower must not be negative")
	}
	if config.WindowSize < 0 {
		return nil, fmt.Errorf("windowSize must not be negative")
	} else if config.WindowSize == 0 {
		a.windowSize = 1
	} else {
		a.windowSize = config.WindowSize
	}

	a.edpReductionMode, err = EdpReductionModeParse(config.ReductionMode)
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *MedianAggregator) MetricAdd(ccMessage lp.CCMessage) {
	m, ok := metricCheckAndGet(ccMessage, a.deviceType)
	if !ok {
		return
	}

	_, isPower := a.powerMetrics[m.MetricName]
	_, isPerformance := a.performanceMetrics[m.MetricName]
	isEndOfBatch := m.MetricName == "ccmc-end"

	if !isPower && !isPerformance && !isEndOfBatch {
		// metric is not of interest
		return
	}

	// Incoming metrics are put into 'metricsIncoming'.
	// We do this in order to have a 'batch' of metrics, which were
	// likely obtained during the same ccmc run. That way the calculations
	// do not use metrics, which don't belong together.
	// As soon as 'batch' is complete, we put the calculated power and performance
	// into our history buffer 'metricsStaged'.
	//
	// It is important that ccmc-{begin,end} marker metrics are being send
	// Otherwise ccmc will not function, since it doesn't reliably know, which
	// metrics belong together

	if isEndOfBatch {
		a.metricsStage(m.HostName)
		a.metricsReadyUpdate()
	} else {
		if a.metricsIncoming[m.HostName] == nil {
			a.metricsIncoming[m.HostName] = make(map[DeviceIdString]map[MetricNameString]TargetMetricValue)
		}
		if a.metricsIncoming[m.HostName][m.DeviceId] == nil {
			a.metricsIncoming[m.HostName][m.DeviceId] = make(map[MetricNameString]TargetMetricValue)
		}
		if _, ok := a.metricsIncoming[m.HostName][m.DeviceId][m.MetricName]; ok {
			cclog.Warnf("Received metrics '%v' without getting a 'ccmc-end' metric inbetween. Make sure markers-enabled is set in your cc-metric-collector config", m.MetricName)
		}
		a.metricsIncoming[m.HostName][m.DeviceId][m.MetricName] = m
	}
}

func (a *MedianAggregator) MetricsReady() bool {
	return a.metricsReady
}

func (a *MedianAggregator) metricsStage(hostname HostNameString) {
	cclog.ComponentDebugf("MedianAggregator", "Trying to stage metrics")

	// Check if all metrics for te specified host were received.
	// If so, stage them. If not, don't do anything and return.
	samplesToStage := make(map[DeviceIdString]MetricStaged)

	for deviceId, _ := range a.devicesToManage[hostname] {
		metricReceived := func(metricName MetricNameString) (float64, bool) {
			deviceIds, ok := a.metricsIncoming[hostname]
			if !ok {
				return 0.0, false
			}
			metrics, ok := deviceIds[deviceId]
			if !ok {
				return 0.0, false
			}
			metric, ok := metrics[metricName]
			if !ok {
				return 0.0, false
			}
			return metric.Value, true
		}

		powerSumForDevice := 0.0
		for reqMetricName, reqMetricRange := range a.powerMetrics {
			powerMetricValue, ok := metricReceived(reqMetricName)
			if !ok {
				return
			}
			powerSumForDevice += reqMetricRange.Apply(powerMetricValue)
		}

		performanceProductForDevice := 1.0
		for reqMetricName, reqMetricRange := range a.powerMetrics {
			performanceMetricValue, ok := metricReceived(reqMetricName)
			if !ok {
				return
			}
			performanceProductForDevice *= reqMetricRange.Apply(performanceMetricValue)
		}

		samplesToStage[deviceId] = MetricStaged{
			power:       powerSumForDevice,
			performance: performanceProductForDevice,
		}
	}

	// Now that we know we have all required metrics for a host, actually stage the metrics
	metricsStagedForHost, ok := a.metricsStaged[hostname]
	if !ok {
		metricsStagedForHost = make([]map[DeviceIdString]MetricStaged, 0)
	}
	metricsStagedForHost = append(metricsStagedForHost, samplesToStage)
	a.metricsStaged[hostname] = metricsStagedForHost
}

func (a *MedianAggregator) metricsReadyUpdate() {
	// Check if we are now ready for the final aggregation.
	// This is the case, when we have at least N samples, where N is the configured
	// window size.
	// Should the window size drift too apart from multiple hosts (> 2), we issue
	// a warning.

	minSampleCount := math.MaxInt
	maxSampleCount := 0
	for _, metricsStagedForHost := range a.metricsStaged {
		minSampleCount = min(minSampleCount, len(metricsStagedForHost))
		maxSampleCount = max(maxSampleCount, len(metricsStagedForHost))
	}

	if maxSampleCount-minSampleCount > 2 {
		cclog.Warnf("Detected desync in metrics received. Received %d batches of metrics from one host, "+
			"while only receiving %d from another. "+
			"Is cc-metric-collector configured correctly on all nodes?", maxSampleCount, minSampleCount)
	}

	if minSampleCount > a.windowSize {
		a.metricsReady = true
	}
}

func (a *MedianAggregator) GetEdpPerTarget() map[Target]float64 {
	// calculate energy delay product (EDP) per host and per device
	edp := make(map[string]map[string]float64)

	for hostname, samplesForHost := range a.metricsStaged {
		edp[string(hostname)] = make(map[string]float64)

		samples := make(map[DeviceIdString]struct {
			power       []float64
			performance []float64
		}, len(samplesForHost))

		for _, deviceSamples := range samplesForHost {
			for deviceId, deviceSample := range deviceSamples {
				samplesForDevice, ok := samples[deviceId]
				if !ok {
					samplesForDevice.power = make([]float64, 0)
					samplesForDevice.performance = make([]float64, 0)
				}
				samplesForDevice.power = append(samplesForDevice.power, deviceSample.power)
				samplesForDevice.performance = append(samplesForDevice.performance, deviceSample.performance)
				samples[deviceId] = samplesForDevice
			}
		}

		for deviceId, samplesForDevice := range samples {
			if len(samplesForDevice.power) == 0 || len(samplesForDevice.performance) == 0 {
				cclog.Panicf("BUG: empty sample list")
			}

			powerMedian, _ := util.Median(samplesForDevice.power)
			performanceMedian, _ := util.Median(samplesForDevice.performance)
			edp[string(hostname)][string(deviceId)] = (powerMedian + a.basePower) / (performanceMedian * performanceMedian)
		}
	}

	return DeviceEdpToTargetEdp(edp, a.edpReductionMode)
}

func (a *MedianAggregator) MetricsReset() {
	a.metricsIncoming = make(map[HostNameString]map[DeviceIdString]map[MetricNameString]TargetMetricValue)
	a.metricsStaged = make(map[HostNameString][]map[DeviceIdString]MetricStaged)
	a.metricsReady = false
}
