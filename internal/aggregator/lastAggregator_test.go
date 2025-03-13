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

func createMessage(name string, value float64, hostname string) (ccmessage.CCMessage, error) {
	tags := make(map[string]string)
	ts = ts.Add(60 * time.Second)
	tags["hostname"] = hostname
	tags["type"] = "node"
	tags["type-id"] = "0"
	m, err := ccmessage.NewMetric(name, tags, nil, value, ts)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func TestInit(t *testing.T) {
	testconfig := `{
        "type": "last",
        "energy": "cpu_energy",
        "instructions": "instructions"
      }`

	cclog.Init("debug", false)
	_, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}
}

func TestAddSingle(t *testing.T) {
	ts = time.Now()
	testconfig := `{
        "type": "last",
        "energy": "cpu_energy",
        "instructions": "instructions"
      }`

	cclog.Init("debug", false)
	ag, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}

	var energy, instructions float64

	for i := 0; i < 10; i++ {
		energy = 100.0 + float64(i)
		m, err := createMessage("cpu_energy", energy, "m1203")
		if err != nil {
			t.Errorf("failed to create message: %v", err.Error())
			return
		}
		ag.Add(m)
		instructions = 10.0 * float64(i)
		m, err = createMessage("instructions", instructions, "m1203")
		if err != nil {
			t.Errorf("failed to create message: %v", err.Error())
			return
		}
		ag.Add(m)
	}

	in := ag.Get()

	if in["m1203"] != energy/instructions {
		t.Errorf("expected %f, got %f", energy/instructions, in["m1203"])
	}
}

func TestAddMultiple(t *testing.T) {
	ts = time.Now()
	testconfig := `{
        "type": "last",
        "energy": "cpu_energy",
        "instructions": "instructions"
      }`

	cclog.Init("debug", false)
	ag, err := NewLastAggregator(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init LasAggregator: %v", err.Error())
		return
	}

	var energy, instructions [4]float64
	hosts := [4]string{"m1203", "m1204", "m1205", "m1206"}

	for i := 0; i < 10; i++ {
		for r, host := range hosts {
			energy[r] = 100.0 + float64(r)*2 + float64(i)
			m, err := createMessage("cpu_energy", energy[r], host)
			if err != nil {
				t.Errorf("failed to create message: %v", err.Error())
				return
			}
			ag.Add(m)
			instructions[r] = 10.0 * float64(r) * 3.0 * float64(i)
			m, err = createMessage("instructions", instructions[r], host)
			if err != nil {
				t.Errorf("failed to create message: %v", err.Error())
				return
			}
			ag.Add(m)
		}
	}

	in := ag.Get()

	if in["m1205"] != energy[2]/instructions[2] {
		t.Errorf("expected %f, got %f", energy[2]/instructions[2], in["m1205"])
	}
}
