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
	Scope             string          `json:"scope"`
	AggCfg            json.RawMessage `json:"aggregator"`
	CtrlConfig        json.RawMessage `json:"control"`
	IntervalConverged string          `json:"intervalConverged"`
	IntervalSearch    string          `json:"intervalSearch"`
}

type JobManager struct {
	wg          *sync.WaitGroup
	Done        chan bool
	Input       chan lp.CCMessage
	output      chan lp.CCMessage
	interval    time.Duration
	targets     []string
	ctrlTargets map[string][]string
	aggregator  aggregator.Aggregator
	optimizer   map[string]Optimizer
	control     controller.Controller
	ticker      time.Ticker
	started     bool
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
	j.ctrlTargets = make(map[string][]string)
	// for _, r := range resources {
	//   if isSocketMetric(r.Name) || isAcceleratorMetric(r.Name) {
	//     j.targets = append(j.targets, r.Name)
	//   }
	// }

	j.aggregator = aggregator.New(c.AggCfg)
	j.control, _ = controller.NewCcController(c.CtrlConfig)

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
		r.Done <- true
		<-r.Done
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

	go func(done chan bool, wg *sync.WaitGroup) {
		warmUpDone := false
		for {
			select {
			case <-done:
				wg.Done()
				close(done)
				return
			case <-j.Input:
				for i := 0; i < 10 && len(j.Input) > 0; i++ {
					j.aggregator.Add(<-j.Input)
				}

			case <-j.ticker.C:
				input := j.aggregator.Get()

				for !warmUpDone {
					for _, t := range j.targets {
						out, warmUpDone := j.optimizer[t].Start(input[t])
					}
				}

				// TODO: Handle error and treat case no new values are available
				for _, t := range j.targets {
					out := j.optimizer[t].Update(input[t])

					for _, ct := range j.ctrlTargets[t] {
						j.control.Set(ct, out)
					}
				}
			}
		}
	}(j.Done, j.wg)
}
