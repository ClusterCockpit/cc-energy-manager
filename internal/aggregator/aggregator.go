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
)

const (
	EdpReduceArithMean EdpReductionMode = iota
	EdpReduceGeomMean
	EdpReduceHarmMean
	EdpReduceMin
	EdpReduceMax
	EdpReduceInvalid

	HarmMeanMin = 0.000000001
)

type EdpReductionMode int

type Aggregator interface {
	// Add a metric to this aggregator
	AggregateMetric(m lp.CCMessage)
	// Get the current EDP vor all known targets.
	// The returned map maps all available target names to EDP.
	// If metrics for certain hosts or devices have not yet been received,
	// they will not be present in this map. Your code should handle this accordingly
	GetEdpPerTarget() map[Target]float64
}

type Target struct {
	HostName string
	DeviceId string
}

var (
	MinInit = math.Inf(1)
)

func New(rawConfig json.RawMessage) (Aggregator, error) {
	var cfg struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("Error while unmarshaling raw config JSON: %v", err)
	}

	switch cfg.Type {
	case "last":
		return NewLastAggregator(rawConfig)
	case "median":
		return NewMedianAggregator(rawConfig)
	default:
		return nil, fmt.Errorf("Unknown aggregator %s", cfg.Type)
	}
}

func valueToFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	}
	return math.NaN(), fmt.Errorf("cannot convert %v to float64", value)
}

func JobScopeTarget() Target {
	return Target{}
}

func NodeScopeTarget(hostname string) Target {
	return Target{HostName: hostname}
}

func DeviceScopeTarget(hostname string, deviceId string) Target {
	return Target{HostName: hostname, DeviceId: deviceId}
}

func (t Target) String() string {
	if t.HostName == "" {
		return ""
	}
	if t.DeviceId != "" {
		return fmt.Sprintf("%s/%s", t.HostName, t.DeviceId)
	}
	return t.HostName
}

func EdpReductionModeParse(configString string) (EdpReductionMode, error) {
	if configString == "" {
		return EdpReduceArithMean, nil
	}

	availModes := map[string]EdpReductionMode{
		"arithmeticMean": EdpReduceArithMean,
		"geometricMean":  EdpReduceGeomMean,
		"harmonicMean":   EdpReduceHarmMean,
		"min":            EdpReduceMin,
		"max":            EdpReduceMax,
	}

	mode, ok := availModes[configString]
	if !ok {
		return EdpReduceInvalid, fmt.Errorf("Invalid reduction mode '%s'. Available modes: %+v", configString, availModes)
	}

	return mode, nil
}

func edpAccumulatorInit(edpReductionMode EdpReductionMode) float64 {
	if edpReductionMode == EdpReduceGeomMean {
		return 1.0
	}
	if edpReductionMode == EdpReduceMin {
		return MinInit
	}
	return 0.0
}

func edpAccumulate(edpAcc, edp float64, edpReductionMode EdpReductionMode) float64 {
	switch edpReductionMode {
	case EdpReduceArithMean:
		return edpAcc + edp
	case EdpReduceGeomMean:
		return edpAcc * edp
	case EdpReduceHarmMean:
		if edp < HarmMeanMin {
			cclog.Errorf("Cannot accumulate with EDP of %f (non positive)", edp)
			return edpAcc
		}
		return edpAcc + (1.0 / edp)
	case EdpReduceMin:
		return min(edpAcc, edp)
	case EdpReduceMax:
		return max(edpAcc, edp)
	default:
		cclog.Panicf("BUG: Invalid edp reduction mode: %d", int(edpReductionMode))
	}

	return 0.0
}

func edpReduce(edpAcc float64, degree int, edpReductionMode EdpReductionMode) float64 {
	if degree <= 0 {
		cclog.Panicf("Cannot reduce EDP (%f) with mode %d by non-positive degree: %d", edpAcc, int(edpReductionMode), degree)
	}

	switch edpReductionMode {
	case EdpReduceArithMean:
		return edpAcc / float64(degree)
	case EdpReduceGeomMean:
		return math.Pow(edpAcc, 1.0/float64(degree))
	case EdpReduceHarmMean:
		if edpAcc < HarmMeanMin {
			cclog.Errorf("Cannot reduce with EDP of %f (non positive)", edpAcc)
			return 0.0
		}
		return 1.0 / (edpAcc / float64(degree))
	case EdpReduceMin:
		if edpAcc == MinInit {
			return 0.0
		}
		return edpAcc
	case EdpReduceMax:
		return edpAcc
	default:
		cclog.Panicf("BUG: Invalid edp reduction mode: %d", int(edpReductionMode))
	}

	return 0.0
}

// This function receives a map `map[hostname]map[deviceId]edp` and
// returns a `map[targetName]edp`.
// All target scopes are calculated, regardless of the actual scope used.
// The receiver of the return value just picks the required target type.
// The upper scopes are calculated by averaging the values. Perhaps we should make
// this configurable.
func DeviceEdpToTargetEdp(edpMap map[string]map[string]float64, edpReductionMode EdpReductionMode) map[Target]float64 {
	jobEdp := edpAccumulatorInit(edpReductionMode)
	jobNumDevices := 0

	targetEdp := make(map[Target]float64)

	for hostname, deviceIdToEdp := range edpMap {
		hostEdp := edpAccumulatorInit(edpReductionMode)
		hostNumDevices := 0

		for deviceId, edp := range deviceIdToEdp {
			hostEdp = edpAccumulate(hostEdp, edp, edpReductionMode)
			hostNumDevices++
			targetEdp[DeviceScopeTarget(hostname, deviceId)] = edp
		}

		jobEdp = edpAccumulate(jobEdp, hostEdp, edpReductionMode)
		jobNumDevices += hostNumDevices

		if hostNumDevices > 0 {
			hostEdp = edpReduce(hostEdp, hostNumDevices, edpReductionMode)
			targetEdp[NodeScopeTarget(hostname)] = hostEdp
		}
	}

	if jobNumDevices > 0 {
		jobEdp = edpReduce(jobEdp, jobNumDevices, edpReductionMode)
		targetEdp[JobScopeTarget()] = jobEdp
	}

	return targetEdp
}

func checkAndGetMetricFields(m lp.CCMessage, wantedDeviceType string) (hostname string, deviceId string, value float64, ok bool) {
	var err error

	// Mind the named return values and the naked return statements!
	if !m.IsMetric() {
		cclog.Debugf("Unable to aggregate non-metric message: %+v", m)
		return
	}

	hostname, ok = m.GetTag("hostname")
	if !ok {
		cclog.Errorf("Unable to aggregate metric without hostname: %+v", m)
		return
	}

	valueAny, _ := m.GetMetricValue()
	value, err = valueToFloat64(valueAny)
	if err != nil {
		cclog.Errorf("Unable to parse float (%s) from message: %+v", err, m)
		ok = false
		return
	}

	deviceType, ok := m.GetTag("type")
	if !ok {
		cclog.Errorf("Unable to aggregate metric: missing field type: %+v", m)
		return
	}

	if deviceType != wantedDeviceType {
		// this log message can probably be removed, since this case is not unusual
		// cclog.Debugf("Ignoring metric of non-matching type '%s', wanted '%s'", deviceType, a.deviceType)
		ok = false
		return
	}

	deviceId, ok = m.GetTag("type-id")
	if !ok {
		cclog.Errorf("Unable to aggregate metric: missing field type-id: %+v", m)
		return
	}

	return
}
