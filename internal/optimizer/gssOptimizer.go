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
	NarrowDown Mode = iota
	BroadenUp
	BroadenDown
)

type gssOptimizerConfig struct {
	Tolerance int `json:"tolerance"`
	Borders   struct {
		Lower int `json:"lower"`
		Upper int `json:"upper"`
	} `json:"borders,omitempty"`
}

//  |<-----    h    ----->|
//  *-------*-----*-------*
//  a       c     d       b

type gssOptimizer struct {
	fa           float64 // value at a
	fb           float64 // value at b
	fc           float64 // value at c
	fd           float64 // value at d
	a            float64 // outer interval lower point
	b            float64 // outer interval upper point
	c            float64 // inner interval lower point
	d            float64 // inner interval upper point
	h            float64 // outer interval distance
	lowerBarrier float64 // never go below this barrier
	upperBarrier float64 // never go above this barrier
	tol          float64
	mode         Mode
	probe        float64
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
	x = o.NarrowDown()
	return x, true
}

func (o *gssOptimizer) IsConverged() bool {
	return o.h < 4*o.tol
}

func (o *gssOptimizer) Update(fx float64) (x float64) {
	switch {
	case o.c == o.probe:
		cclog.Debugf("Set fc to %f", fx)
		o.fc = fx
	case o.d == o.probe:
		cclog.Debugf("Set fd to %f", fx)
		o.fd = fx
	case o.a == o.probe:
		cclog.Debugf("Set fa to %f", fx)
		o.fa = fx
	case o.b == o.probe:
		cclog.Debugf("Set fb to %f", fx)
		o.fb = fx
	}
	cclog.Debugf("Interval distance %f", o.h)

	switch o.mode {
	case NarrowDown:
		return o.NarrowDown()

	case BroadenDown:
		return o.BroadenDown()

	default:
		return o.BroadenUp()
	}
}

func (o *gssOptimizer) DumpState(position string) {
	cclog.Debugf("State at %s", position)
	cclog.Debugf("\tfa: %f", o.fa)
	cclog.Debugf("\tfc: %f", o.fc)
	cclog.Debugf("\tfd: %f", o.fd)
	cclog.Debugf("\tfb: %f", o.fb)
}

func (o *gssOptimizer) NarrowDown() float64 {
	if o.fc < o.fd {
		if o.h < o.tol { // expand toward lower: c becomes new d and a becomes new c, new probe a
			// Before:
			//              *-------*---*-------*
			//              a       c   d       b
			// After:
			// *------------*-------*-----------*
			// a            c       d           b
			// |
			// Probe
			o.h = o.h * phi
			o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, o.fc, o.a, nan, o.b-o.h, nan
			o.mode = BroadenDown
			o.probe = o.a
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
			o.h = o.h * invphi
			o.b, o.c, o.fc, o.d, o.fd = o.d, o.a+invphi2*o.h, nan, o.c, o.fc
			o.probe = o.c
			cclog.Debugf("\tsearch lower: try next %f", o.probe)
		}
	} else {
		if o.h < o.tol { // expand toward higher: d becomes new c and b becomes new d, new probe b
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			// *-----------*-------*-----------*
			// a           c       d           b
			//                                 |
			//                                 Probe
			o.h = o.h * phi
			o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, o.fd, o.b, nan, o.a+o.h, nan
			o.mode = BroadenUp
			o.probe = o.b
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
			o.h = o.h * invphi
			o.a, o.c, o.fc, o.d, o.fd = o.c, o.d, o.fd, o.b-invphi2*o.h, nan
			o.probe = o.d
			cclog.Debugf("\tsearch higher: try next %f", o.probe)
		}
	}
	return o.probe
}

func (o *gssOptimizer) BroadenDown() float64 {
	if math.IsNaN(o.fc) {
		// in initial broaden down both fa and fc are unknown so do a second broaden down
		// Before:
		//              *-------*---*-------*
		//              a       c   d       b
		// After:
		// *------------*-------*-----------*
		// a            c       d           b
		// |
		// Probe
		o.h = o.h * phi
		o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, o.fc, o.a, o.fa, o.b-o.h, nan
		o.probe = o.a
		cclog.Debugf("\tinitial broaden down: %f", o.probe)
	} else {
		if o.fa < o.fc { // minimum still outside, further expand
			// Before:
			//              *-------*---*-------*
			//              a       c   d       b
			// After:
			// *------------*-------*-----------*
			// a            c       d           b
			// |
			// Probe
			o.h = o.h * phi
			o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, nan, o.a, o.fa, o.b-o.h, nan
			o.probe = o.a
			cclog.Debugf("\tbroaden down: %f", o.probe)
		} else { // captured minimum, switch to NarrowDown downwards
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			// *---*---*---*
			// a   c   d   b
			//     |
			//     Probe
			o.h = o.h * invphi
			o.b, o.c, o.fc, o.d, o.fd, o.fa, o.fb = o.d, o.a+invphi2*o.h, nan, o.c, o.fc, nan, nan
			o.mode = NarrowDown
			o.probe = o.c
			cclog.Debugf("\tSwitch to Narrow down from Broaden down: %f", o.probe)
		}
	}

	return o.probe
}

func (o *gssOptimizer) BroadenUp() float64 {
	if math.IsNaN(o.fd) {
		// in initial broaden up both fb and fd are unknown so do a second broaden up
		// Before:
		// *-------*---*-------*
		// a       c   d       b
		// After:
		// *-----------*-------*-----------*
		// a           c       d           b
		//                                 |
		//                                 Probe
		o.h = o.h * phi
		o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, o.fd, o.b, o.fb, o.a+o.h, nan
		o.probe = o.b
		cclog.Debugf("\tbroaden up: %f", o.probe)
	} else {
		if o.fb < o.fd { // minimum still outside, further expand
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			// *-----------*-------*-----------*
			// a           c       d           b
			//                                 |
			//                                 Probe
			o.h = o.h * phi
			o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, nan, o.b, o.fb, o.a+o.h, nan
			o.probe = o.b
			cclog.Debugf("\tbroaden up: %f", o.probe)
		} else { // captured minimum, switch to NarrowDown upwards
			// Before:
			// *-------*---*-------*
			// a       c   d       b
			// After:
			//         *---*---*---*
			//         a   c   d   b
			//                 |
			//                 Probe
			o.h = o.h * invphi
			o.a, o.d, o.fd, o.c, o.fc, o.fa, o.fb = o.c, o.a+invphi2*o.h, nan, o.d, o.fd, nan, nan
			o.mode = NarrowDown
			o.probe = o.d
			cclog.Debugf("\tSwitch to Narrow down from Broaden up: %f", o.probe)
		}
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
		a:            float64(c.Borders.Lower),
		b:            float64(c.Borders.Upper),
		c:            nan,
		d:            nan,
		lowerBarrier: float64(c.Borders.Lower),
		upperBarrier: float64(c.Borders.Upper),
		fa:           nan,
		fb:           nan,
		fc:           nan,
		fd:           nan,
		mode:         NarrowDown,
		probe:        nan,
		tol:          float64(c.Tolerance),
	}

	return &o, err
}
