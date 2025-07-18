// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"math"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

type Mode int

// enum optimizing strategy
const (
	Narrow Mode = iota
	BroadenUp
	BroadenDown
)

type gssOptimizerConfig struct {
	Tolerance int `json:"tolerance"`
	Borders   struct {
		Lower int `json:"lower"`
		Upper int `json:"upper"`
	} `json:"borders,omitempty"`
	BroadenLimit int     `json:"broadenLimit"`
	FudgeFactor  float64 `json:"fudgeFactor"`
}

//  |<-----    h    ----->|
//  *-------*-----*-------*
//  a       c     d       b

type gssOptimizer struct {
	fa              float64 // value at a
	fb              float64 // value at b
	fc              float64 // value at c
	fd              float64 // value at d
	a               float64 // outer interval lower point
	b               float64 // outer interval upper point
	c               float64 // inner interval lower point
	d               float64 // inner interval upper point
	h               float64 // outer interval distance
	hMax            float64
	lowerBarrier    float64 // never go below this barrier
	upperBarrier    float64 // never go above this barrier
	lowerBarrierCfg float64 // original config value
	upperBarrierCfg float64 // original config value
	tol             float64
	counter         int
	mode            Mode
	probe           float64
	broadenLimit    int
	fudgeFactor     float64
}

var (
	sqrt5   = math.Sqrt(5)
	phi     = (sqrt5 + 1) / 2 //# phi
	invphi  = (sqrt5 - 1) / 2 //# 1/phi
	invphi2 = (3 - sqrt5) / 2 //# 1/phi^2
	nan     = math.NaN()
)

func (o *gssOptimizer) Start(fx float64) (x float64, ok bool) {
	o.h = o.b - o.a

	if o.d == o.probe {
		cclog.Debugf("Set fd to %f", fx)
		o.fd = fx
	}
	if o.c == o.probe {
		cclog.Debugf("Set fc to %f", fx)
		o.fc = fx
	}

	if math.IsNaN(o.fc) {
		o.c = o.a + invphi2*o.h
		o.probe = o.c
		x = o.probe
		cclog.Debugf("Set probe c to %f", x)
		return x, false
	}
	if math.IsNaN(o.fd) {
		o.d = o.b - invphi2*o.h
		o.probe = o.d
		x = o.probe
		cclog.Debugf("Set probe d to %f", x)
		return x, false
	}

	cclog.Debugf("Startup finished: c(%f):%f d(%f):%f", o.c, o.fc, o.d, o.fd)
	x = o.Narrow()
	return x, true
}

func (o *gssOptimizer) IsConverged() bool {
	return o.h < 2*o.tol
}

func (o *gssOptimizer) Update(fx float64) float64 {
	offset := fx * o.fudgeFactor
	switch {
	case o.c == o.probe:
		cclog.Debugf("Set fc to %f", fx)
		o.fc = fx
	case o.d == o.probe:
		cclog.Debugf("Set fd to %f", fx)
		o.fd = fx - offset
	case o.a == o.probe:
		cclog.Debugf("Set fa to %f", fx)
		o.fa = fx
	case o.b == o.probe:
		cclog.Debugf("Set fb to %f", fx)
		o.fb = fx
	}
	cclog.Debugf("Interval distance %f", o.h)

	if (o.b > o.upperBarrier) && o.counter == 0 { // we hit upper barrier, broaden toward lower
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		//         -   -       -
		// After:
		// *-------*---*-------*
		// a       c   d       b
		// -       -   -       -
		// |
		// Probe
		o.counter = o.broadenLimit // Lets hope we find the new minimum within 10 iterations if there is any
		o.fc = nan
		o.probe = o.a
		o.mode = BroadenDown
		cclog.Debugf("\tHit upper barrier. Broaden down: %f", o.probe)
		return o.probe
	} else if o.a < o.lowerBarrier && o.counter == 0 { // we hit lower barrier, broaden toward higher
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		// -       -   -
		// After:
		// *-------*---*-------*
		// a       c   d       b
		// -       -   -       -
		//                     |
		//                     Probe
		o.counter = o.broadenLimit
		o.fd = nan
		o.probe = o.b
		o.mode = BroadenUp
		cclog.Debugf("\tHit lower barrier. Broaden up: %f", o.probe)
		return o.probe
	}

	switch o.mode {
	case Narrow:
		return o.Narrow()

	case BroadenDown:
		return o.BroadenDown()

	default:
		return o.BroadenUp()
	}
}

func (o *gssOptimizer) GetBordersCfg() (float64, float64) {
	cclog.Debugf("gss cfg --> [%v, %v]", o.lowerBarrierCfg, o.upperBarrierCfg)
	return o.lowerBarrierCfg, o.upperBarrierCfg
}

func (o *gssOptimizer) GetBordersCur() (float64, float64) {
	cclog.Debugf("gss cur --> [%v, %v]", o.lowerBarrier, o.upperBarrier)
	return o.lowerBarrier, o.upperBarrier
}

func (o *gssOptimizer) SetBorders(lower, upper float64) {
	o.lowerBarrier = max(o.lowerBarrierCfg, lower)
	o.upperBarrier = min(o.upperBarrierCfg, upper)
}

func (o *gssOptimizer) DumpState(position string) {
	cclog.Debugf("State at %s", position)
	cclog.Debugf("\tfa(%f): %f", o.a, o.fa)
	cclog.Debugf("\tfc(%f): %f", o.c, o.fc)
	cclog.Debugf("\tfd(%f): %f", o.d, o.fd)
	cclog.Debugf("\tfb(%f): %f", o.b, o.fb)
	cclog.Debugf("\tcounter: %d", o.counter)
	cclog.Debugf("\tmode: %d", int(o.mode))
}

func (o *gssOptimizer) contractTowardsHigher() {
	o.h = o.h * invphi
	o.a, o.c, o.fc, o.d, o.fd, o.fb = o.c, o.d, o.fd, o.b-invphi2*o.h, nan, nan
	o.probe = o.d
	cclog.Debugf("\tsearch higher: try next %f", o.probe)
}

func (o *gssOptimizer) contractTowardsLower() {
	o.h = o.h * invphi
	o.b, o.c, o.fc, o.d, o.fd, o.fa = o.d, o.a+invphi2*o.h, nan, o.c, o.fc, nan
	o.probe = o.c
	cclog.Debugf("\tsearch lower: try next %f", o.probe)
}

func (o *gssOptimizer) broadenDown() {
	o.h = o.h * phi
	o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, o.fc, o.a, o.fa, o.b-o.h, nan
	o.probe = o.a
}

func (o *gssOptimizer) broadenUp() {
	o.h = o.h * phi
	o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, o.fd, o.b, o.fb, o.a+o.h, nan
	o.probe = o.b
}

func (o *gssOptimizer) Narrow() float64 {
	o.counter = 0
	if math.IsNaN(o.fc) {
		fmt.Printf("fc NAN\n")
	}
	if math.IsNaN(o.fd) {
		fmt.Printf("fd NAN\n")
	}
	if o.fc < o.fd {
		if o.h < o.tol { // expand toward lower: c becomes new d and a becomes new c, new probe a
			// Set fc to nan to not get stuck with old function value
			// Before:
			// *------------*-------*-----------*
			// a            c       d           b
			//              -       -
			// After:
			// *------------*-------*-----------*
			// a            c       d           b
			// -            -       -
			// |
			// Probe
			o.fc = nan
			o.probe = o.a
			o.mode = BroadenDown
			cclog.Debugf("\tSwitch to broaden down: %f", o.probe)
		} else { // contract towards lower:  d becomes new b and c becomes new d, new probe c
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			// *---*---*---*
			// a   c   d   b
			//     |
			//     Probe
			o.contractTowardsLower()
		}
	} else {
		if o.h < o.tol { // expand toward higher: all points stay the same, new probe b
			// Set fd to nan to not get stuck with old function value
			// Before:
			// *-----------*-------*-----------*
			// a           c       d           b
			//             -       -
			// After:
			// *-----------*-------*-----------*
			// a           c       d           b
			//             -                   -
			//                                 |
			//                                 Probe
			o.fd = nan
			o.probe = o.b
			o.mode = BroadenUp
			cclog.Debugf("\tSwitch to broaden up: %f", o.probe)
		} else { // contract towards higher:  c becomes new a and d becomes new c, new probe d
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			//         *---*---*---*
			//         a   c   d   b
			//                 |
			//                 Probe
			o.contractTowardsHigher()
		}
	}
	return o.probe
}

func (o *gssOptimizer) BroadenDown() float64 {
	if o.counter > 0 {
		if o.counter == 1 {
			o.contractTowardsLower()
			o.mode = Narrow
			o.counter--
			return o.probe
		} else {
			o.counter--
		}
	}

	if math.IsNaN(o.fc) {
		o.broadenDown()
		cclog.Debugf("\t initial broaden down: %f", o.probe)
		return o.probe
	}

	if math.IsNaN(o.fa) {
		cclog.Errorf("cannot compare against 'fa', which is NaN")
	}

	if o.fa < o.fc && o.h < o.hMax { // minimum still outside, further expand
		// Before:
		//              *-------*---*-------*
		//              a       c   d       b
		//              -       -
		// After:
		// *------------*-------*-----------*
		// a            c       d           b
		// -            -       -
		// |
		// Probe
		o.broadenDown()
		cclog.Debugf("\tbroaden down: %f", o.probe)
	} else { // captured minimum, switch to Narrow downwards
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		// -       -   -
		// After:
		// *---*---*---*
		// a   c   d   b
		// -   -   -   -
		//     |
		//     Probe
		o.contractTowardsLower()
		o.mode = Narrow
		cclog.Debugf("\tSwitch to Narrow from Broaden down: %f", o.probe)
	}

	return o.probe
}

func (o *gssOptimizer) BroadenUp() float64 {
	if o.counter > 0 {
		if o.counter == 1 {
			o.contractTowardsHigher()
			o.mode = Narrow
			o.counter--
			return o.probe
		} else {
			o.counter--
		}
	}

	if math.IsNaN(o.fd) {
		o.broadenUp()
		cclog.Debugf("\t initial broaden up: %f", o.probe)
		return o.probe
	}

	if math.IsNaN(o.fb) {
		cclog.Errorf("cannot compare against 'fb', which is NaN")
	}

	if o.fb < o.fd && o.h < o.hMax { // minimum still outside, further expand
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		//             -       -
		// After:
		// *-----------*-------*-----------*
		// a           c       d           b
		//             -       -           -
		//                                 |
		//                                 Probe
		o.broadenUp()
		cclog.Debugf("\tbroaden up: %f", o.probe)
	} else { // captured minimum, switch to Narrow upwards
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		//         -   -       -
		// After:
		//         *---*---*---*
		//         a   c   d   b
		//         -   -   -   -
		//                 |
		//                 Probe
		o.contractTowardsHigher()
		o.mode = Narrow
		cclog.Debugf("\tSwitch to Narrow from Broaden up: %f", o.probe)
	}

	return o.probe
}

func NewGssOptimizer(config json.RawMessage) (*gssOptimizer, error) {
	var c gssOptimizerConfig
	c.Tolerance = 10
	c.Borders.Lower = 30
	c.Borders.Upper = 800

	err := json.Unmarshal(config, &c)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError("GSS", err.Error())
		return nil, err
	}
	o := gssOptimizer{
		a:               float64(c.Borders.Lower),
		b:               float64(c.Borders.Upper),
		c:               nan,
		d:               nan,
		lowerBarrier:    float64(c.Borders.Lower),
		upperBarrier:    float64(c.Borders.Upper),
		lowerBarrierCfg: float64(c.Borders.Lower),
		upperBarrierCfg: float64(c.Borders.Upper),
		fa:              nan,
		fb:              nan,
		fc:              nan,
		fd:              nan,
		mode:            Narrow,
		probe:           nan,
		hMax:            float64(c.Borders.Upper-c.Borders.Lower) * 0.5,
		tol:             float64(c.Tolerance),
		broadenLimit:    c.BroadenLimit,
		fudgeFactor:     c.FudgeFactor,
	}

	return &o, err
}
