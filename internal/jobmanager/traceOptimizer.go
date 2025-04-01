// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
	"runtime"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

type traceOptimizerConfig struct {
	Borders   struct {
		Lower *float64 `json:"lower"`
		Upper *float64 `json:"upper"`
	} `json:"borders,omitempty"`
	TracePath string `json:"tracePath"`
	TraceResolution int `json:"traceResolution"`
	IterationsPerProbe int `json:"iterationsPerProbe"`
}

type traceOptimizer struct {
	lowerBound float64
	upperBound float64
	traceHandle *os.File
	traceResolution int
	iteration int
	iterationsPerProbe int
	current float64
}

func NewTraceOptimizer(config json.RawMessage) (*traceOptimizer, error) {
	var c traceOptimizerConfig

	err := json.Unmarshal(config, &c)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %w", err)
		cclog.ComponentError("Trace", err.Error())
		return nil, err
	}

	if c.Borders.Lower == nil || c.Borders.Upper == nil {
		return nil, fmt.Errorf("lower/upper missing in optimizer config")
	}

	if c.TracePath == "" {
		return nil, fmt.Errorf("tracePath missing in optimizer config")
	}

	if c.IterationsPerProbe <= 0 {
		c.IterationsPerProbe = 1
	}

	if c.TraceResolution <= 2 {
		c.TraceResolution = 2
	}

	o := traceOptimizer{
		lowerBound: *c.Borders.Lower,
		upperBound: *c.Borders.Upper,
		iterationsPerProbe: c.IterationsPerProbe,
		traceResolution: c.TraceResolution,
	}

	fileName := fmt.Sprintf("trace_%s_*.txt", time.Now().Format(time.RFC3339))
	o.traceHandle, err = os.CreateTemp(c.TracePath, fileName)
	if err != nil {
		return nil, fmt.Errorf("Failed to open trace file: %w", err)
	}

	runtime.SetFinalizer(&o, func(o *traceOptimizer) {
		o.traceHandle.Close()
	})

	return &o, nil
}

func (o *traceOptimizer) Start(_ float64) (float64, bool) {
	o.current = o.lowerBound
	o.iteration = 0
	return o.current, true
}

func (o *traceOptimizer) Update(pdp float64) float64 {
	if o.iteration <= 0 {
		o.iteration = 1
		return o.current
	}

	t := time.Now().Format(time.TimeOnly)
	o.traceHandle.Write([]byte(fmt.Sprintf("[%s] Limit=%f --> PDP=%f\n", t, o.current, pdp)))
	o.traceHandle.Sync()

	if o.iteration == o.iterationsPerProbe {
		o.current += (o.upperBound - o.lowerBound) / float64(o.traceResolution)
		if o.current > o.upperBound {
			o.current = o.lowerBound
		}
		o.iteration = 0
		return o.current
	}

	o.iteration += 1
	return o.current
}
