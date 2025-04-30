// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"
	"fmt"
	"math"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type Aggregator interface {
	// Add a metric to this aggregator
	AggregateMetric(m lp.CCMessage)
	// Get the current PDP vor all known targets.
	// The returned map maps all available target names to PDP.
	// If metrics for certain hosts or devices have not yet been received,
	// they will not be present in this map. Your code should handle this accordingly
	GetPdpPerTarget() map[Target]float64
}

type Target struct {
	HostName string
	DeviceId string
}

func New(rawConfig json.RawMessage) Aggregator {
	var err error
	var ag Aggregator

	var cfg struct {
		Type string `json:"type"`
	}

	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return nil
	}

	switch cfg.Type {
	case "last":
		ag, _ = NewLastAggregator(rawConfig)
	case "median":
		ag, _ = NewMedianAggregator(rawConfig)
	default:
		cclog.Errorf("Unknown aggregator %s", cfg.Type)
	}

	return ag
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

// This function receives a map `map[hostname]map[deviceId]pdp` and
// returns a `map[targetName]pdp`.
// All target scopes are calculated, regardless of the actual scope used.
// The upper scopes are calculated by averaging the values. Perhaps we should make
// this configurable.
func DevicePdpToTargetPdp(pdpMap map[string]map[string]float64, useMax bool) map[Target]float64 {
	jobPdp := 0.0
	jobNumDevices := 0

	targetPdp := make(map[Target]float64)

	for hostname, deviceIdToPdp := range pdpMap {
		hostPdp := 0.0
		hostNumDevices := 0

		for deviceId, pdp := range deviceIdToPdp {
			if useMax {
				hostPdp = max(hostPdp, pdp)
			} else {
				hostPdp += pdp
			}
			hostNumDevices++

			targetPdp[DeviceScopeTarget(hostname, deviceId)] = pdp
		}

		if useMax {
			jobPdp = max(jobPdp, hostPdp)
		} else {
			jobPdp += hostPdp
		}
		jobNumDevices += hostNumDevices

		if hostNumDevices > 0 {
			if !useMax {
				hostPdp /= float64(hostNumDevices)
			}
			targetPdp[NodeScopeTarget(hostname)] = hostPdp
		}
	}

	if jobNumDevices > 0 {
		if !useMax {
			jobPdp /= float64(jobNumDevices)
		}
		targetPdp[JobScopeTarget()] = jobPdp
	}

	return targetPdp
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

	value, err = valueToFloat64(m.GetMetricValue())
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
