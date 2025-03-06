// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

func TestInit(t *testing.T) {
	testconfig := `{
      "tol": 10,
      "borders": {
        "lower_outer": 123,
        "upper_outer": 890
      }}`

	cclog.Init("debug", false)
	_, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
}

func TestStart(t *testing.T) {
	testconfig := `{
      "tol": 10,
      "borders": {
        "lower_outer": 60,
        "upper_outer": 600
      }}`

	cclog.Init("debug", false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v int) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}

	in := 400.0
	var out int
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	// t.Errorf("failed to init GssOptimizer")
}

func TestOptimize(t *testing.T) {
	testconfig := `{
       "tol": 2,
       "borders": {
         "lower_outer": 60,
         "upper_outer": 600
       }}`

	cclog.Init("debug", false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v int) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}

	in := 400.0
	var out int
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	fmt.Printf("it %d in: %f out: %d\n", i, in, out)

	for {
		if i > 20 {
			t.Errorf("failed to find minimum")
			return
		}

		out = o.Update(in)
		in = f(out)
		fmt.Printf("it %d in: %f out: %d\n", i, in, out)

		if out > 243 && out < 246 {
			break
		}

		i++
	}

	// if out > 100 {
	// 	t.Errorf("failed to calculate minimum")
	// }
}

func TestMovingMinimum(t *testing.T) {
	testconfig := `{
       "tol": 2,
       "borders": {
         "lower_outer": 60,
         "upper_outer": 600
       }}`

	cclog.Init("debug", false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v int) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}
	fn := func(v int) float64 {
		x := float64(v)
		y := 0.004*x*x - 2.9*x + 700

		return y
	}

	in := 400.0
	var out int
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	fmt.Printf("it %d in: %f out: %d\n", i, in, out)

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
		fmt.Printf("it %d in: %f out: %d\n", i, in, out)

		if out > 395 && out < 405 {
			break
		}

		i++
	}

	// if out > 100 {
	// 	t.Errorf("failed to calculate minimum")
	// }
}

func TestMovingTwice(t *testing.T) {
	testconfig := `{
       "tol": 2,
       "borders": {
         "lower_outer": 60,
         "upper_outer": 600
       }}`

	cclog.Init("debug", false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}

	f := func(v int) float64 {
		x := float64(v)
		y := 0.006*x*x - 2.9*x + 400

		return y
	}
	fn := func(v int) float64 {
		x := float64(v)
		y := 0.004*x*x - 2.9*x + 700

		return y
	}

	in := 400.0
	var out int
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = f(out)
	}
	i := 0
	fmt.Printf("it %d in: %f out: %d\n", i, in, out)

	for {
		if i > 60 {
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
		fmt.Printf("it %d in: %f out: %d\n", i, in, out)

		if i > 45 && (out > 238 && out < 242) {
			break
		}

		i++
	}
}
