// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

type testOptimizerConfig struct {
	Borders struct {
		Lower *float64 `json:"lower"`
		Upper *float64 `json:"upper"`
	} `json:"borders,omitempty"`
}

type testOptimizer struct {
	lowerBound float64
	upperBound float64
	current    float64
	delta      float64
}

func NewTestOptimizer(config json.RawMessage) (*testOptimizer, error) {
	var c testOptimizerConfig

	err := json.Unmarshal(config, &c)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %w", err)
		cclog.ComponentError("Test", err.Error())
		return nil, err
	}

	if c.Borders.Lower == nil || c.Borders.Upper == nil {
		return nil, fmt.Errorf("lower/upper missing in optimizer config")
	}

	o := testOptimizer{
		lowerBound: *c.Borders.Lower,
		upperBound: *c.Borders.Upper,
		current:    *c.Borders.Lower,
		delta:      (*c.Borders.Upper - *c.Borders.Lower) / 10.0,
	}

	return &o, nil
}

func (o *testOptimizer) Start(_ float64) (float64, bool) {
	return o.current, true
}

func (o *testOptimizer) Update(_ float64) float64 {
	oldCurrent := o.current
	o.current += o.delta
	if o.current <= o.lowerBound {
		o.delta = -o.delta
		o.current = o.lowerBound
	} else if o.current >= o.upperBound {
		o.delta = -o.delta
		o.current = o.upperBound
	}
	return oldCurrent
}
