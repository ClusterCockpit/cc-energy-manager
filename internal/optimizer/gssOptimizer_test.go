// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

const loglevel = "info"

// TODO:
// Implement test with noisy and flat function

func TestInit(t *testing.T) {
	testconfig := `{
      "tol": 10,
      "borders": {
        "lower": 123,
        "upper": 890
      }}`

	cclog.Init(loglevel, false)
	_, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
}

func TestStart(t *testing.T) {
	testconfig := `{
      "tolerance": 10,
      "borders": {
        "lower": 60,
        "upper": 600
      }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	// t.Errorf("failed to init GssOptimizer")
}

func TestOptimize(t *testing.T) {
	testconfig := `{
       "tolerance": 2,
       "borders": {
         "lower": 60,
         "upper": 600
       }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

	for {
		if i > 20 {
			t.Errorf("failed to find minimum")
			return
		}

		out = o.Update(in)
		in = f(out)
		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

		if o.IsConverged() {
			break
		}

		i++
	}

	if out < 241 || out > 244 {
		t.Errorf("failed to calculate minimum: %f", out)
	}
}

func TestMovingMinimum(t *testing.T) {
	testconfig := `{
        "tolerance": 2,
        "borders": {
          "lower": 60,
          "upper": 600
        }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}
	fn := func(v float64) float64 {
		x := float64(v)
		y := 0.004*x*x - 2.9*x + 700

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

	for {
		if i > 40 {
			t.Errorf("failed to find minimum")
			return
		}

		out = o.Update(in)
		if i < 12 {
			in = f(out)
		} else {
			in = fn(out)
		}
		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

		if out > 360 && out < 363 && o.IsConverged() {
			break
		}

		i++
	}

	if out < 361 && out > 365 {
		t.Errorf("failed to calculate minimum: %f", out)
	}
}

func TestMovingTwice(t *testing.T) {
	testconfig := `{
        "tolerance": 2,
        "borders": {
          "lower": 60,
          "upper": 600
        }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}
	fn := func(v float64) float64 {
		x := float64(v)
		y := 0.004*x*x - 2.9*x + 700

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

	for {
		if i > 80 {
			t.Errorf("failed to find minimum")
			return
		}

		out = o.Update(in)
		if i < 12 {
			in = f(out)
		} else if i < 34 {
			in = fn(out)
		} else {
			in = f(out)
		}
		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)

		if i > 70 && (out > 238 && out < 242) && o.IsConverged() {
			break
		}

		i++
	}

	if out < 238 && out > 242 {
		t.Errorf("failed to calculate minimum")
	}
}

func TestLowerBarrier(t *testing.T) {
	testconfig := `{
       "tolerance": 2,
       "borders": {
         "lower": 60,
         "upper": 600
       }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := 0.8*x + 20

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
	for {
		out = o.Update(in)
		in = f(out)

		if out < 55 {
			t.Errorf("exceed lower barrier %d: %f", i, out)
		}
		if i > 70 {
			break
		}

		i++
	}
}

func TestUpperBarrier(t *testing.T) {
	testconfig := `{
        "tol": 2,
        "borders": {
          "lower": 60,
          "upper": 600
        }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v float64) float64 {
		x := float64(v)
		y := -0.6*x + 600

		return y
	}

	in := 400.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
	for {
		out = o.Update(in)
		in = f(out)

		if out > 610 {
			t.Errorf("exceed upper barrier %d: %f", i, out)
		}
		if i > 70 {
			break
		}

		i++
	}
}
