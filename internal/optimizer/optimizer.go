// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
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
	Interval   string `json:"interval"`
	MaxProcess int    `json:"maxprocess"`
}

type optimizer struct {
	wg       sync.WaitGroup
	globalwg *sync.WaitGroup
	done     chan bool
	ident    string
	config   optimizerConfig
	input    chan lp.CCMessage
	output   chan lp.CCMessage
	interval time.Duration
	job      ccspecs.BaseJob
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

func (o *gssOptimizer) Init(ident string, wg *sync.WaitGroup,
	metadata ccspecs.BaseJob, config json.RawMessage,
) error {
	o.ident = fmt.Sprintf("GssOptimizer(%s)", ident)
	o.globalwg = wg
	o.done = make(chan bool)
	o.started = false
	o.data = make(map[string]gssOptimizer)

	o.config.MaxProcess = 10
	o.config.Limits.Min = 140
	o.config.Limits.Max = 220
	o.config.Limits.Idle = 140
	o.config.Limits.Step = 1
	o.config.Borders.Lower_inner = 170558
	o.config.Borders.Lower_outer = 140000
	o.config.Borders.Upper_inner = 189442
	o.config.Borders.Upper_outer = 220000

	err := json.Unmarshal(config, &o.config)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError(o.ident, err.Error())
		return err
	}

	t, err := time.ParseDuration(o.config.Interval)
	if err != nil {
		err := fmt.Errorf("failed to parse interval %s: %v", o.config.Interval, err.Error())
		cclog.ComponentError(o.ident, err.Error())
		return err
	}
	o.interval = t

	for _, r := range metadata.Resources {
		if _, ok := o.data[r.Hostname]; !ok {
			k := gssOptimizer{
				calls:   0,
				edplast: float64(0.0),
				fxa:     o.config.Borders.Lower_outer,
				fxc:     o.config.Borders.Lower_inner,
				fxb:     o.config.Borders.Upper_outer,
				fxd:     o.config.Borders.Upper_inner,
				mode:    NarrowDown,
				limits: gssOptimizerLimits{
					min:  o.config.Limits.Min,
					max:  o.config.Limits.Max,
					step: o.config.Limits.Step,
					idle: o.config.Limits.Idle,
				},
			}
			// TODO: Ask Host for real limits and stuff
			o.data[r.Hostname] = k
		}
	}

	o.InitCache(metadata, o.config.optimizerConfig)

	return nil
}

func (os *optimizer) AddInput(input chan lp.CCMessage) {
	os.input = input
}

func (os *optimizer) AddOutput(output chan lp.CCMessage) {
	os.output = output
}

func (os *optimizer) Close() {
	if os.started {
		cclog.ComponentDebug(os.ident, "Sending Done")
		os.done <- true
		<-os.done
		cclog.ComponentDebug(os.ident, "STOPPING Timer")
		// os.ticker.Stop()
	}
	cclog.ComponentDebug(os.ident, "Waiting for optimizer to exit")
	os.wg.Wait()
	cclog.ComponentDebug(os.ident, "signalling closing")
	os.globalwg.Done()
	cclog.ComponentDebug(os.ident, "CLOSE")
}

func (os *optimizer) Start() {
	os.wg.Add(1)
	// Ticker for running the optimizer
	os.ticker = *time.NewTicker(os.interval)
	os.started = true
	results := make(map[string][]float64)

	go func(done chan bool, wg *sync.WaitGroup) {
		for {
			select {
			case <-done:
				wg.Done()
				close(done)
				cclog.ComponentDebug(os.ident, "DONE")
				return
			case <-os.input:
				for i := 0; i < len(os.input) && i < os.config.MaxProcess; i++ {
					aggregate(<-os.input)
				}

			case <-os.ticker.C:
				// Iterate over the host and their result lists
				for h, rlist := range results {
					if len(rlist) == 0 {
						continue
					}
					sort.Float64s(rlist)
					median := rlist[int(len(rlist)/2)]
					// Delete the result list of the host
					results[h] = results[h][:0]
					// Run the calculator
					d := os.data[h]

					if os.data[h].edplast > 0 && d.calls >= 2 {
						cclog.ComponentDebug(os.ident, "Analyse cache with GSS")
						d.powercap = d.Update(d.powercap, median)
						d.edplast = median
						cclog.ComponentDebug(os.ident, "New powercap", d.powercap)
						out, err := lp.NewPutControl(os.config.Control.Name, map[string]string{
							"hostname": h,
							"type":     os.config.Control.Type,
							"type-id":  "0",
						}, nil, fmt.Sprintf("%d", d.powercap), time.Now())
						if err == nil {
							cclog.ComponentDebug(os.ident, out.String())
							os.output <- out
						}
					} else {
						cclog.ComponentDebug(os.ident, "Saving EDP for next round")
						d.edplast = median
					}

					d.calls++
					os.data[h] = d
				}
			}
		}
	}(os.done, &os.wg)
	cclog.ComponentDebug(os.ident, "START")
}
