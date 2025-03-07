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
	energy       float64
	instructions float64
	metrics      map[string]string
}

func NewLastAggregator(rawConfig json.RawMessage) (*LastAggregator, error) {
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

	return ag, nil
}

func (fsa *LastAggregator) Add(lp.CCMessage) {
}

func (fsa *LastAggregator) Get() (map[string]float64, error) {
	return nil, nil
}
