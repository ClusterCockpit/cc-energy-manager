// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"
	"testing"
	"time"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	ccmessage "github.com/ClusterCockpit/cc-lib/ccMessage"
)

var ts time.Time

func TestInit(t *testing.T) {
	testconfig := `{
        "type": "last",
        "powerMetric": "cpu_power",
        "performanceMetric": "instructions",
		"deviceType": "socket"
      }`

	cclog.Init("debug", false)
	_, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}
}

func TestAggregateSingleDevice(t *testing.T) {
	ts = time.Now()
	testconfig := `{
        "type": "last",
        "powerMetric": "cpu_energy",
        "performanceMetric": "instructions",
		"deviceType": "socket"
      }`

	cclog.Init("debug", false)
	ag, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}

	power := 100.0
	instructions := 10.0

	for i := 0; i < 10; i++ {
		power += 2.45
		instructions += 1

		tags := map[string]string{"hostname": "m1203", "type": "socket", "type-id": "0"}
		m, err := ccmessage.NewMetric("cpu_energy", tags, nil, power, ts)
		if err != nil {
			t.Errorf("failed to create message: %v", err.Error())
			return
		}

		ag.AggregateMetric(m)

		m, err = ccmessage.NewMetric("instructions", tags, nil, instructions, ts)
		if err != nil {
			t.Errorf("failed to create message: %v", err.Error())
			return
		}

		ag.AggregateMetric(m)
	}

	edp := ag.GetEdpPerTarget()

	target := Target{HostName: "m1203", DeviceId: "0"}
	if edp[target] != power/instructions {
		t.Errorf("expected %f, got %f", power/instructions, edp[target])
	}

	target = Target{HostName: "m1203"}
	if edp[target] != power/instructions {
		t.Errorf("expected %f, got %f", power/instructions, edp[target])
	}

	target = Target{}
	if edp[target] != power/instructions {
		t.Errorf("expected %f, got %f", power/instructions, edp[target])
	}
}

func TestAggregateMultipleHosts(t *testing.T) {
	ts = time.Now()
	testconfig := `{
        "type": "last",
        "powerMetric": "cpu_energy",
        "performanceMetric": "instructions",
		"deviceType": "socket"
      }`

	cclog.Init("debug", false)
	ag, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}

	power := [4]float64{124.3, 252.1, 133.0, 122.1}
	instructions := [4]float64{24.3, 52.1, 33.0, 22.1}
	hosts := [4]string{"m1203", "m1204", "m1205", "m1206"}

	for i := 0; i < 10; i++ {
		for hostIdx, host := range hosts {
			power[hostIdx] += 2.45
			instructions[hostIdx] += 1

			tags := map[string]string{"hostname": host, "type": "socket", "type-id": "0"}
			m, err := ccmessage.NewMetric("cpu_energy", tags, nil, power[hostIdx], ts)
			if err != nil {
				t.Errorf("failed to create message: %v", err.Error())
				return
			}

			ag.AggregateMetric(m)

			m, err = ccmessage.NewMetric("instructions", tags, nil, instructions[hostIdx], ts)
			if err != nil {
				t.Errorf("failed to create message: %v", err.Error())
				return
			}

			ag.AggregateMetric(m)
		}
	}

	edp := ag.GetEdpPerTarget()

	for i, host := range hosts {
		target := Target{HostName: host, DeviceId: "0"}
		if edp[target] != power[i]/instructions[i] {
			t.Errorf("expected %f, got %f", power[i]/instructions[i], edp[target])
		}

		target = Target{HostName: host}
		if edp[target] != power[i]/instructions[i] {
			t.Errorf("expected %f, got %f", power[i]/instructions[i], edp[target])
		}
	}
}
