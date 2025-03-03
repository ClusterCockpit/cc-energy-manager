// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"math"
	"strings"
	"sync"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type Mode int

// enums used to choose the optimizing strategy
const (
	NarrowDown Mode = iota
	BroadenUp
	BroadenDown
)

type gssOptimizerConfig struct {
	optimizerConfig
	MaxProcess int `json:"max_process,omitempty"`
	Limits     struct {
		Min  int `json:"minimum"`
		Max  int `json:"maximum"`
		Step int `json:"step"`
		Idle int `json:"idle"`
	} `json:"limits"`
	Borders struct {
		Lower_outer int `json:"lower_outer"`
		Lower_inner int `json:"lower_inner"`
		Upper_outer int `json:"upper_outer"`
		Upper_inner int `json:"upper_inner"`
	} `json:"borders,omitempty"`
}

type gssOptimizerLimits struct {
	min, max, idle, step int
}

type gssOptimizer struct {
	fxa      int
	fxb      int
	fxc      int
	fxd      int
	xa       float64 // outer interval
	xb       float64 // outer interval
	xc       float64 // inner interval
	xd       float64 // inner interval
	mode     Mode
	limits   gssOptimizerLimits
	edplast  float64
	previous int
	calls    int64
}

var GOLDEN_RATIO float64 = (math.Sqrt(5) + 1) / 2

func (d *gssOptimizer) Update(input float64) (output int) {
	switch {
	case d.fxa == d.previous:
		d.xa = input
	case d.fxc == d.previous:
		d.xc = input
	case d.fxd == d.previous:
		d.xd = input
	case d.fxb == d.previous:
		d.xb = input
	}

	switch d.mode {
	case NarrowDown:
		output = d.NarrowDown()
	case BroadenDown:
		output = d.BroadenDown()
	default:
		output = d.BroadenUp()
	}

	d.previous = output
	return output
}

func (d *gssOptimizer) SwitchToNarrowDown() {
	d.mode = NarrowDown
	a := int(float64((d.fxd)-(d.fxc)) * GOLDEN_RATIO)
	d.fxa = d.fxc - a
	d.xa = 0.0
	d.fxb = d.fxd + a
	d.xb = 0.0
}

func (d *gssOptimizer) NarrowDown() int {
	if d.xc == 0 {
		return d.fxc
	}
	if d.xd == 0 {
		return d.fxd
	}
	border := int(float64((d.fxb)-(d.fxc)) / GOLDEN_RATIO)
	new_c := int((GOLDEN_RATIO - 1) * float64((d.fxd)-(d.fxc)))

	if d.xd < d.xc && new_c >= d.limits.step {
		// Search higher
		d.fxa = d.fxc
		d.xa = d.xc
		d.fxc = d.fxd
		d.xc = d.xd
		d.fxd = d.fxa + border
		return d.fxd
	} else if d.xc <= d.xd && new_c >= d.limits.step {
		// Search lower
		d.fxb = d.fxd
		d.xb = d.xd
		d.fxd = d.fxc
		d.xd = d.xc
		d.fxc = d.fxb - border
		return d.fxc
	} else {
		// Terminate narrow-down if step is too small
		d.fxb = d.fxd + new_c
		d.xb = 0.0
		d.fxa = d.fxc - new_c
		d.xa = 0.0
		if d.mode == BroadenUp {
			d.mode = BroadenDown
			return d.fxa
		} else {
			d.mode = BroadenUp
			return d.fxb
		}
	}
}

func (d *gssOptimizer) BroadenDown() int {
	// Calculate ratio (after shifting borders)
	a := int((GOLDEN_RATIO - 1) * float64(d.fxd-d.fxc))
	b := int((GOLDEN_RATIO) * float64(d.fxb-d.fxd))
	//	limits =

	if d.xb < d.xd && float64(d.fxb)+(GOLDEN_RATIO+1)*float64(b) <= float64(d.limits.max) {
		// Search higher
		d.fxd = d.fxb
		d.xd = d.xb
		d.fxb = d.fxd + a
		return d.fxb
	} else if d.xb < d.xd && b-((d.fxb)-(d.fxd)) >= d.limits.step {
		// Nearing limits -> reset exponential growth
		d.fxc = d.fxd
		d.xc = d.xd
		d.fxd = d.fxb
		d.xd = d.xb
		d.fxa = d.fxd - b
		d.xa = 0.0
		d.fxb = d.fxc + b
		return d.fxb
	} else if d.xd <= d.xb && float64((d.fxb)-(d.fxd))/(GOLDEN_RATIO+1) >= float64(d.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64((d.fxb)-(d.fxd)))
		d.fxc = d.fxd
		d.xc = d.xd
		d.fxd = d.fxb - a
		d.SwitchToNarrowDown()
		return d.fxd
	} else {
		// Moved past sweetspot or hitting step size
		if ((d.fxb) - (d.fxd)) > d.limits.step {
			// Move lower border up, if step size allows it
			// This speeds up the narrow-down
			d.fxc = d.fxd
			d.xc = d.xd
			d.fxd = d.fxb
			d.xd = d.xb
		}
		d.SwitchToNarrowDown()
		return d.fxc
	}
}

func (d *gssOptimizer) BroadenUp() int {
	// Calculate ratio (after shifting borders)
	a := int((GOLDEN_RATIO - 1) * float64((d.fxd)-(d.fxc)))
	b := int((GOLDEN_RATIO) * float64((d.fxc)-(d.fxa)))
	//	limits = self._limits[d.mode]
	if d.xa < d.xc && d.fxa-int(GOLDEN_RATIO+1)*b >= d.limits.min {
		// Search lower
		d.fxc = d.fxa
		d.xc = d.xa
		d.fxa = d.fxc - a
		d.fxd = d.fxc
		d.xd = d.xc
		d.fxc = d.fxa
		d.xc = d.xa
		d.fxb = d.fxc + b
		d.xb = 0.0
		d.fxa = d.fxd - b
		return d.fxa
	} else if d.xc <= d.xa && float64((d.fxc)-(d.fxa))/(GOLDEN_RATIO+1) >= float64(d.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64((d.fxc)-(d.fxa)))
		d.fxd = d.fxc
		d.xd = d.xc
		d.fxc = d.fxa + a
		d.SwitchToNarrowDown()
		return d.fxc
	} else {
		// Moved past sweetspot or hitting step size
		if (d.fxc)-(d.fxa) > d.limits.step {
			// Move upper border down, if step size allows it
			// This speeds up the narrow-down
			d.fxd = d.fxc
			d.xd = d.xc
			d.fxc = d.fxa
			d.xc = d.xa
		}
		d.SwitchToNarrowDown()
		return d.fxd
	}
}

func isSocketMetric(metric string) bool {
	return (strings.Contains(metric, "power") || strings.Contains(metric, "energy") || metric == "mem_bw")
}

func isAcceleratorMetric(metric string) bool {
	return strings.HasPrefix(metric, "acc_")
}

func NewGssOptimizer(ident string,
	wg *sync.WaitGroup,
	metadata ccspecs.BaseJob,
	config json.RawMessage,
) (*gssOptimizer, error) {
	o := new(gssOptimizer)

	err := o.Init(ident, wg, metadata, config)
	if err != nil {
		cclog.ComponentError("failed to initialize GssOptimizer")
		return nil, err
	}
	wg.Add(1)

	return o, err
}
