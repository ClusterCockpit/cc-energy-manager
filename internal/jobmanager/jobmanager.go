// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package jobmanager

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClusterCockpit/cc-energy-manager/internal/aggregator"
	"github.com/ClusterCockpit/cc-energy-manager/internal/controller"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type optimizerConfig struct {
	Scope   string          `json:"scope"`
	AggCfg  json.RawMessage `json:"aggregator"`
	Control struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"control"`
	IntervalConverged string `json:"intervalConverged"`
	IntervalSearch    string `json:"intervalSearch"`
}

type JobManager struct {
	wg         *sync.WaitGroup
	Done       chan bool
	Input      chan lp.CCMessage
	output     chan lp.CCMessage
	interval   time.Duration
	targets    []string
	aggregator aggregator.Aggregator
	optimizer  map[string]Optimizer
	control    controller.Controller
	ticker     time.Ticker
	started    bool
}

type Optimizer interface {
	Start(float64) (int, bool)
	Update(float64) int
	IsConverged() bool
}

func NewJobManager(wg *sync.WaitGroup, resources []*ccspecs.Resource,
	config json.RawMessage,
) (*JobManager, error) {
	var c optimizerConfig

	err := json.Unmarshal(config, &c)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}
	j := JobManager{
		wg:        wg,
		Done:      make(chan bool),
		started:   false,
		optimizer: make(map[string]Optimizer),
	}

	t, err := time.ParseDuration(c.IntervalSearch)
	if err != nil {
		err := fmt.Errorf("failed to parse interval %s: %v", c.IntervalSearch, err.Error())
		cclog.ComponentError("JobManager", err.Error())
		return nil, err
	}
	j.interval = t
	// TODO: Generate target list from job resources using scope
	j.targets = make([]string, 0)
	// for _, r := range resources {
	//   if isSocketMetric(r.Name) || isAcceleratorMetric(r.Name) {
	//     j.targets = append(j.targets, r.Name)
	//   }
	// }

	j.aggregator = aggregator.New(c.AggCfg)

	for _, t := range j.targets {
		j.optimizer[t], err = NewGssOptimizer(config)
		if err != nil {
			err := fmt.Errorf("failed to initialize GSSOptimizer: %v", err.Error())
			cclog.ComponentError("JobManager", err.Error())
			return nil, err
		}
	}

	return &j, nil
}

func isSocketMetric(metric string) bool {
	return (strings.Contains(metric, "power") || strings.Contains(metric, "energy") || metric == "mem_bw")
}

func isAcceleratorMetric(metric string) bool {
	return strings.HasPrefix(metric, "acc_")
}

func (j *JobManager) AddInput(input chan lp.CCMessage) {
	j.Input = input
}

func (j *JobManager) AddOutput(output chan lp.CCMessage) {
	j.output = output
}

// TODO: Fix correct shutdown
func (r *JobManager) Close() {
	if r.started {
		cclog.ComponentDebug("JobManager", "Sending Done")
		r.done <- true
		<-r.done
		cclog.ComponentDebug("JobManager", "STOPPING Timer")
		// os.ticker.Stop()
	}
	cclog.ComponentDebug("JobManager", "Waiting for optimizer to exit")
	r.wg.Done()
	cclog.ComponentDebug("JobManager", "CLOSE")
}

func (j *JobManager) Start() {
	j.wg.Add(1)
	// Ticker for running the optimizer
	j.ticker = *time.NewTicker(j.interval)
	j.started = true
	ok := false

	go func(done chan bool, wg *sync.WaitGroup) {
		for {
			select {
			case <-done:
				wg.Done()
				close(done)
				return
			case <-j.input: // TODO: Is this correct? Or is a value lost?
				for i := 0; i < len(j.input) && i < 10; i++ {
					j.aggregator.Add(<-j.input)
				}

			case <-j.ticker.C:
				input, _ := j.aggregator.Get()

				if !ok { // TODO: Implement warmup
					for !ok {
						for _, t := range j.targets {
							out, ok := j.optimizer[t].Start(input[t])
						}
					}
				}
				// TODO: Handle error and treat case no new values are available
				for _, t := range j.targets {
					out := j.optimizer[t].Update(input[t])
					// TODO: Implement controller package
					j.output <- j.control.Set(t, out)
				}
			}
		}
	}(j.done, j.wg)
}
