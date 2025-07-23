// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

type SamplePoint struct {
	PowerLimit float64
	PDP        float64
}

const loglevel = "debug"

var (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	Gray   = "\033[37m"
	White  = "\033[97m"
)

func TestInit(t *testing.T) {
	testconfig := `{
      "tolerance": 2,
      "count": 2,
      "fudgeFactor": 0.05,
      "borders": {
        "lower": 20,
        "upper": 890
      }}`

	cclog.Init(loglevel, false)
	_, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
}

// func TestStart(t *testing.T) {
// 	testconfig := `{
//       "tolerance": 10,
//       "borders": {
//         "lower": 60,
//         "upper": 600
//       }}`
//
// 	cclog.Init(loglevel, false)
// 	o, err := NewGssOptimizer(json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
//
// 	f := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.006*x*x - 2.9*x + 400
//
// 		return y
// 	}
//
// 	in := 400.0
// 	var out float64
// 	ok := false
//
// 	for !ok {
// 		out, ok = o.Start(in)
// 		in = f(out)
// 	}
// 	// t.Errorf("failed to init GssOptimizer")
// }
//
// func TestOptimize(t *testing.T) {
// 	testconfig := `{
//         "tolerance": 2,
//         "borders": {
//           "lower": 60,
//           "upper": 600
//         }}`
//
// 	cclog.Init(loglevel, false)
// 	o, err := NewGssOptimizer(json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
//
// 	f := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.006*x*x - 2.9*x + 400
//
// 		return y
// 	}
//
// 	in := 400.0
// 	var out float64
// 	ok := false
//
// 	for !ok {
// 		out, ok = o.Start(in)
// 		in = f(out)
// 	}
// 	i := 0
// 	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 	for {
// 		if i > 20 {
// 			t.Errorf("failed to find minimum")
// 			return
// 		}
//
// 		noise := rand.Float64()*(in*0.1) - (in * 0.05)
// 		out = o.Update(in + noise)
// 		in = f(out)
// 		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 		if o.IsConverged() {
// 			break
// 		}
//
// 		i++
// 	}
//
// 	if out < 240 || out > 260 {
// 		t.Errorf("failed to calculate minimum: %f", out)
// 	}
// }

// func TestMovingMinimum(t *testing.T) {
// 	testconfig := `{
//         "tolerance": 2,
//         "borders": {
//           "lower": 60,
//           "upper": 600
//         }}`
//
// 	cclog.Init(loglevel, false)
// 	o, err := NewGssOptimizer(json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
//
// 	f := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.006*x*x - 2.9*x + 400
//
// 		return y
// 	}
// 	fn := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.004*x*x - 2.9*x + 700
//
// 		return y
// 	}
//
// 	in := 400.0
// 	var out float64
// 	ok := false
//
// 	for !ok {
// 		out, ok = o.Start(in)
// 		in = f(out)
// 	}
// 	i := 0
// 	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 	for {
// 		if i > 40 {
// 			t.Errorf("failed to find minimum")
// 			return
// 		}
//
// 		out = o.Update(in)
// 		if i < 12 {
// 			in = f(out)
// 		} else {
// 			in = fn(out)
// 		}
// 		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 		if out > 390 && out < 410 && o.IsConverged() {
// 			break
// 		}
//
// 		i++
// 	}
//
// 	if out < 390 && out > 410 {
// 		t.Errorf("failed to calculate minimum: %f", out)
// 	}
// }
//
// func TestMovingTwice(t *testing.T) {
// 	testconfig := `{
//         "tolerance": 2,
//         "borders": {
//           "lower": 60,
//           "upper": 600
//         }}`
//
// 	cclog.Init(loglevel, false)
// 	o, err := NewGssOptimizer(json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
//
// 	f := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.006*x*x - 2.9*x + 400
//
// 		return y
// 	}
// 	fn := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.004*x*x - 2.9*x + 700
//
// 		return y
// 	}
//
// 	in := 400.0
// 	var out float64
// 	ok := false
//
// 	for !ok {
// 		out, ok = o.Start(in)
// 		in = f(out)
// 	}
// 	i := 0
// 	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 	for {
// 		if i > 80 {
// 			t.Errorf("failed to find minimum")
// 			return
// 		}
//
// 		out = o.Update(in)
// 		if i < 12 {
// 			in = f(out)
// 		} else if i < 34 {
// 			in = fn(out)
// 		} else {
// 			in = f(out)
// 		}
// 		cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
//
// 		if i > 70 && (out > 240 && out < 250) && o.IsConverged() {
// 			break
// 		}
//
// 		i++
// 	}
//
// 	if out < 240 && out > 250 {
// 		t.Errorf("failed to calculate minimum")
// 	}
// }
//
// func TestLowerBarrier(t *testing.T) {
// 	testconfig := `{
//        "tolerance": 2,
//        "borders": {
//          "lower": 60,
//          "upper": 600
//        }}`
//
// 	cclog.Init(loglevel, false)
// 	o, err := NewGssOptimizer(json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
//
// 	f := func(v float64) float64 {
// 		x := float64(v)
// 		y := 0.8*x + 20
//
// 		return y
// 	}
//
// 	in := 400.0
// 	var out float64
// 	ok := false
//
// 	for !ok {
// 		out, ok = o.Start(in)
// 		in = f(out)
// 	}
// 	i := 0
// 	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
// 	for {
// 		out = o.Update(in)
// 		in = f(out)
//
// 		if out < 55 {
// 			t.Errorf("exceed lower barrier %d: %f", i, out)
// 		}
// 		if i > 70 {
// 			break
// 		}
//
// 		i++
// 	}
// }

func TestUpperBarrier(t *testing.T) {
	testconfig := `{
         "tolerance": 2,
		 "broadenLimit": 4,
		 "fudgeFactor": 0.05,
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

		if out > 660 {
			t.Errorf("exceed upper barrier %d: %f", i, out)
		}
		if i > 70 {
			break
		}

		i++
	}
}

func TestNoise(t *testing.T) {
	testconfig := `{
         "tolerance": 2,
		 "broadenLimit": 4,
		 "fudgeFactor": 0.05,
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

	in := 100.0
	var out float64
	ok := false

	for !ok {
		out, ok = o.Start(in)
		in = 100
	}
	i := 0
	cclog.Debugf("it %d in: %f out: %f\n", i, in, out)
	for {
		out = o.Update(in)
		noise := rand.Float64()*(in*0.1) - (in * 0.05)

		if out < 550 {
			in = 100 + noise
		} else {
			in = 150 + noise
		}

		if out > 600 {
			t.Errorf("exceed upper barrier %d: %f", i, out)
		}
		if i > 70 {
			break
		}

		cclog.Infof("it %d in: %f out: %f\n", i, in, out)
		i++
	}
}

func TestUpperBarrierAndMove(t *testing.T) {
	testconfig := `{
         "tolerance": 1,
         "count": 2,
         "fudgeFactor": 0.05,
         "borders": {
           "lower": 20,
           "upper": 500
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
	fmin := func(v float64) float64 {
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
		out = o.Update(in)
		if i < 30 {
			in = f(out)
		} else {
			in = fmin(out)
		}

		if out > 660 {
			t.Errorf("exceed upper barrier %d: %f", i, out)
		}
		if i > 70 {
			break
		}

		cclog.Infof("%sit %d %sin: %f out: %f\n", Green, i, Reset, in, out)
		i++
	}
}

func TestWarmup(t *testing.T) {
	testconfig := `{
         "tolerance": 2,
         "borders": {
           "lower": 30,
           "upper": 85
         }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init GssOptimizer: %v", err)
	}

	cclog.Debug("=== Testing Warmup ===")

	newLimit, warmup1 := o.Start(42.0)
	newLimit, warmup2 := o.Start(newLimit)
	newLimit, warmup3 := o.Start(newLimit)

	if warmup1 || warmup2 || !warmup3 {
		t.Fatalf("Test didn't warmup after the expected 2 iterations")
	}
}

func TestFirestarterBergamo(t *testing.T) {
	testconfig := `{
         "tolerance": 2,
         "borders": {
           "lower": 50,
           "upper": 400
         }}`

	cclog.Init(loglevel, false)
	o, err := NewGssOptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init GssOptimizer: %v", err)
	}

	cclog.Debug("=== Testing Firestarter Bergamo Samples ===")

	samples := LoadSamples(t, "testdata/FIRESTARTER.bergamo1", 0)

	newLimit, _ := o.Start(42.0)
	newLimit, _ = o.Start(newLimit)

	for i := 0; i < 30; i++ {
		newLimit = o.Update(ProbeSample(t, samples, newLimit))
	}

	if newLimit < 250 || newLimit > 280 {
		t.Errorf("GSS optimizer did not converge FIRESTARTER correctly: %f", newLimit)
	}
}

func LoadSamples(t *testing.T, path string, socket int) []SamplePoint {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Unable to open test sample file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	result := make([]SamplePoint, 0)

	for scanner.Scan() {
		line := scanner.Text()
		elements := strings.Split(line, ";")
		if len(elements) != 8 {
			continue
		}
		if elements[1] == "POWER_LIMIT" || elements[0] == "SOCKET" {
			continue
		}
		socketTest, err := strconv.Atoi(strings.TrimSpace(elements[0]))
		if err != nil {
			t.Fatal(err)
		}
		if socket != socketTest {
			continue
		}
		powerLimit, err := strconv.ParseFloat(strings.TrimSpace(elements[1]), 64)
		if err != nil {
			t.Fatal(err)
		}
		pdp, err := strconv.ParseFloat(strings.TrimSpace(elements[4]), 64)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, SamplePoint{PowerLimit: powerLimit, PDP: pdp})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PowerLimit < result[j].PowerLimit
	})

	if len(result) < 10 {
		t.Fatal("Sample testdata contains less than 10 points. Did you specify the right file?")
	}

	return result
}

func ProbeSample(t *testing.T, samples []SamplePoint, powerLimit float64) float64 {
	if len(samples) == 0 {
		return 0.0
	}
	cmpFunc := func(s SamplePoint, t float64) int {
		if s.PowerLimit < t {
			return -1
		}
		if s.PowerLimit > t {
			return 1
		}
		return 0
	}

	// Find nearest sample
	pos, _ := slices.BinarySearchFunc(samples, powerLimit, cmpFunc)
	if pos >= len(samples) {
		return samples[pos-1].PDP
	} else if pos > 0 {
		l := samples[pos-1]
		r := samples[pos]
		if powerLimit-l.PowerLimit < r.PowerLimit-powerLimit {
			return l.PDP
		} else {
			return r.PDP
		}
	} else {
		return samples[pos].PDP
	}
}
