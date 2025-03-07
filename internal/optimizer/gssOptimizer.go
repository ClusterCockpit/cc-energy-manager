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

type gssOptimizer struct {
	fa    float64
	fb    float64
	fc    float64
	fd    float64
	a     float64 // outer interval
	b     float64 // outer interval
	c     float64 // inner interval
	d     float64 // inner interval
	h     float64 // outer interval distance
	tol   float64
	mode  Mode
	probe float64
}

var (
	sqrt5   = math.Sqrt(5)
	phi     = (sqrt5 + 1) / 2 //# phi
	invphi  = (sqrt5 - 1) / 2 //# 1/phi
	invphi2 = (3 - sqrt5) / 2 //# 1/phi^2
	nan     = math.NaN()
)

func (o *gssOptimizer) Start(fx float64) (x int, ok bool) {
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
		x = int(o.probe)
		cclog.Debugf("Set probe c to %d", x)
		return x, false
	}
	if math.IsNaN(o.fd) {
		o.d = o.b - invphi2*o.h
		o.probe = o.d
		x = int(o.probe)
		cclog.Debugf("Set probe d to %d", x)
		return x, false
	}

	cclog.Debugf("Startup finished: c(%f):%f d(%f):%f", o.c, o.fc, o.d, o.fd)
	x = o.NarrowDown()
	return x, true
}

func (o *gssOptimizer) IsConverged() bool {
	if o.h < 4*o.tol {
		return true
	}

	return false
}

func (o *gssOptimizer) Update(fx float64) (x int) {
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

func (o *gssOptimizer) NarrowDown() int {
	if o.fc < o.fd {
		if o.h < o.tol { // expand toward lower: c becomes new d and a becomes new c, new probe a
			o.h = o.h * phi
			o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, o.fc, o.a, nan, o.b-o.h, nan
			o.mode = BroadenDown
			o.probe = o.a
			cclog.Debugf("\tSwitch to broaden down: %f", o.probe)
		} else { // contract towards lower:  d becomes new b and c becomes new d, new probe c
			o.h = o.h * invphi
			o.b, o.c, o.fc, o.d, o.fd = o.d, o.a+invphi2*o.h, nan, o.c, o.fc
			o.probe = o.c
			cclog.Debugf("\tsearch lower: try next %f", o.probe)
		}
	} else {
		if o.h < o.tol { // expand toward higher: d becomes new c and b becomes new d, new probe b
			o.h = o.h * phi
			o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, o.fd, o.b, nan, o.a+o.h, nan
			o.mode = BroadenUp
			o.probe = o.b
			cclog.Debugf("\tSwitch to broaden up: %f", o.probe)
		} else { // contract towards higher:  c becomes new a and d becomes new c
			o.h = o.h * invphi
			o.a, o.c, o.fc, o.d, o.fd = o.c, o.d, o.fd, o.b-invphi2*o.h, nan
			o.probe = o.d
			cclog.Debugf("\tsearch higher: try next %f", o.probe)
		}
	}
	return int(o.probe)
}

func (o *gssOptimizer) BroadenDown() int {
	if math.IsNaN(o.fc) { // treat first step separately
		o.h = o.h * phi
		o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, o.fc, o.a, o.fa, o.b-o.h, nan
		o.probe = o.a
		cclog.Debugf("\tinitial broaden down: %f", o.probe)
	} else {
		if o.fa < o.fc { // minimum still outside, further expand
			o.h = o.h * phi
			o.d, o.fd, o.c, o.fc, o.a, o.fa = o.c, nan, o.a, o.fa, o.b-o.h, nan
			o.probe = o.a
			cclog.Debugf("\tbroaden down: %f", o.probe)
		} else { // captured minimum, switch to NarrowDown downwards
			o.h = o.h * invphi
			o.b, o.c, o.fc, o.d, o.fd, o.fa, o.fb = o.d, o.a+invphi2*o.h, nan, o.c, o.fc, nan, nan
			o.mode = NarrowDown
			o.probe = o.c
			cclog.Debugf("\tSwitch to Narrow down from Broaden down: %f", o.probe)
		}
	}

	return int(o.probe)
}

func (o *gssOptimizer) BroadenUp() int {
	if math.IsNaN(o.fd) { // treat first step separately
		o.h = o.h * phi
		o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, o.fd, o.b, o.fb, o.a+o.h, nan
		o.probe = o.b
		cclog.Debugf("\tbroaden up: %f", o.probe)
	} else {
		if o.fb < o.fd { // minimum still outside, further expand
			o.h = o.h * phi
			o.c, o.fc, o.d, o.fd, o.b, o.fb = o.d, nan, o.b, o.fb, o.a+o.h, nan
			o.probe = o.b
			cclog.Debugf("\tbroaden up: %f", o.probe)
		} else { // captured minimum, switch to NarrowDown upwards
			o.h = o.h * invphi
			o.a, o.d, o.fc, o.c, o.fc, o.fa, o.fb = o.c, o.a+invphi2*o.h, nan, o.d, o.fd, nan, nan
			o.mode = NarrowDown
			o.probe = o.b
			cclog.Debugf("\tSwitch to Narrow down from Broaden up: %f", o.probe)
		}
	}

	return int(o.probe)
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
		a:     float64(c.Borders.Lower),
		b:     float64(c.Borders.Upper),
		c:     nan,
		d:     nan,
		fa:    nan,
		fb:    nan,
		fc:    nan,
		fd:    nan,
		mode:  NarrowDown,
		probe: nan,
		tol:   float64(c.Tolerance),
	}

	return &o, err
}
