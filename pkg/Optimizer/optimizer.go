// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"sync"
	"time"

	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type optimizerConfig struct {
	Type    string   `json:"type"`
	Scope   string   `json:"scope"`
	Metrics []string `json:"metrics"`
	Control struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"control"`
	Interval string `json:"interval"`
}

type optimizer struct {
	wg       sync.WaitGroup
	globalwg *sync.WaitGroup
	done     chan bool
	ident    string
	input    chan lp.CCMessage
	output   chan lp.CCMessage
	metadata ccspecs.BaseJob
	cache    metricCache
	ticker   time.Ticker
	started  bool
}

type Optimizer interface {
	Init(ident string, wg *sync.WaitGroup, metadata ccspecs.BaseJob, config json.RawMessage) error
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	Start()
	Close()
}

func (os *optimizer) AddInput(input chan lp.CCMessage) {
	os.input = input
}

func (os *optimizer) AddOutput(output chan lp.CCMessage) {
	os.output = output
}
