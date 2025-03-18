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
	Energy       string `json:"energy"`
	Instructions string `json:"instructions"`
}

type MedianAggregator struct {
	energy       map[string][]float64
	instructions map[string][]float64
	metrics      map[string]string
	scope        string
}

func NewMedianAggregator(scope string, rawConfig json.RawMessage) (*MedianAggregator, error) {
	ag := &MedianAggregator{}
	var config MedianAggregatorConfig

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
	ag.energy = make(map[string][]float64)
	ag.instructions = make(map[string][]float64)
	ag.scope = scope

	return ag, nil
}

func (a *MedianAggregator) Add(m lp.CCMessage) {
	if !m.IsMetric() {
		return
	}
	metric := m.Name()
	if h, ok := m.GetTag("hostname"); ok {
		switch metric {
		case a.metrics["energy"]:
			value, _ := valueToFloat64(m.GetMetricValue())
			a.energy[h] = append(a.energy[h], value)
		case a.metrics["instructions"]:
			value, _ := valueToFloat64(m.GetMetricValue())
			a.instructions[h] = append(a.instructions[h], value)
		}
	}
}

func (a *MedianAggregator) Get() map[string]float64 {
	edp := make(map[string]float64)
	max := 0.0
	maxHost := ""

	for h, energy := range a.energy {
		if instructions, ok := a.instructions[h]; ok {
			energy, err := util.Median(energy)
			if err != nil {
				cclog.Errorf("medianAggregator > error: %v", err)
			}
			instructions, err := util.Median(instructions)
			if err != nil {
				cclog.Errorf("medianAggregator > error: %v", err)
			}
			edp[h] = energy / instructions
			if instructions > max {
				max = instructions
				maxHost = h
			}
			a.instructions[h] = nil
		}
		a.energy[h] = nil
	}

	edp["job"] = edp[maxHost]

	return edp
}
