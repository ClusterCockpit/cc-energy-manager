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
	Energy       string `json:"energy"`
	Instructions string `json:"instructions"`
}

type LastAggregator struct {
	energy       map[string]float64
	instructions map[string]float64
	metrics      map[string]string
	scope        string
}

func NewLastAggregator(scope string, rawConfig json.RawMessage) (*LastAggregator, error) {
	ag := &LastAggregator{}
	var config LastAggregatorConfig

	if err := json.Unmarshal(rawConfig, &config); err != nil {
		cclog.Warnf("Init() > Unmarshal error: %#v", err)
		return nil, err
	}
	if config.Energy == "" || config.Instructions == "" {
		err := fmt.Errorf("Init() : empty metric configuration")
		cclog.Errorf("Init() > config.Path error: %v", err)
		return nil, err
	}
	ag.metrics = make(map[string]string)
	ag.metrics["energy"] = config.Energy
	ag.metrics["instructions"] = config.Instructions
	ag.energy = make(map[string]float64)
	ag.instructions = make(map[string]float64)
	ag.scope = scope

	return ag, nil
}

func (a *LastAggregator) Add(m lp.CCMessage) {
	if !m.IsMetric() {
		return
	}
	metric := m.Name()
	if h, ok := m.GetTag("hostname"); ok {
		switch metric {
		case a.metrics["energy"]:
			value, _ := valueToFloat64(m.GetMetricValue())
			a.energy[h] = value
		case a.metrics["instructions"]:
			value, _ := valueToFloat64(m.GetMetricValue())
			a.instructions[h] = value
		}
	}
}

func (a *LastAggregator) Get() map[string]float64 {
	edp := make(map[string]float64)
	max := 0.0
	maxHost := ""

	for h, energy := range a.energy {
		if instructions, ok := a.instructions[h]; ok {
			edp[h] = energy / instructions
			if instructions > max {
				max = instructions
				maxHost = h
			}
		}
	}

	edp["job"] = edp[maxHost]

	return edp
}
