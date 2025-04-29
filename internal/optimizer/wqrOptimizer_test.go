// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	//"fmt"
	"bufio"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

var testconfig string = `{
	"lowerBound": 50,
	"upperBound": 400,
	"winMinWidth": 100,
	"winMinSamples": 4,
	"winMaxSamples": 10
}`

func TestWQRInit(t *testing.T) {
	cclog.Init("debug", false)
	_, err := NewWQROptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init WQROptimizer: %v", err)
	}
}

func TestWQRInsertSample(t *testing.T) {
	cclog.Init("debug", false)
	o, err := NewWQROptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init WQROptimizer: %v", err)
	}

	o.InsertSample(300.0, 0.0)
	o.InsertSample(310.0, 1.0)
	o.InsertSample(320.0, 2.0)
	o.InsertSample(330.0, 3.0)
	o.InsertSample(305.0, 5.0)
	o.InsertSample(307.0, 6.0)

	expectedPowerLimit := []float64{300.0, 305.0, 307.0, 310.0, 320.0, 330.0}
	expectedEDP := []float64{0.0, 5.0, 6.0, 1.0, 2.0, 3.0}
	for i, v := range o.samples {
		if v.PowerLimit != expectedPowerLimit[i] {
			t.Errorf("Expected PowerLimit %f at index %d, found %f", expectedPowerLimit[i], i, v.PowerLimit)
		}
		if v.EDP != expectedEDP[i] {
			t.Errorf("Expected EDP %f at index %d, found %f", expectedEDP[i], i, v.EDP)
		}
	}
}

func TestWQRCleanupOldSamples(t *testing.T) {
	cclog.Init("debug", false)
	o, err := NewWQROptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init WQROptimizer: %v", err)
	}

	totalSamples := 50
	for i := 0; i < totalSamples; i++ {
		edp := float64(i)
		power := o.lowerBound + float64(i)/float64(totalSamples-1)*(o.upperBound-o.lowerBound)
		o.InsertSample(power, edp)
	}

	o.CleanupOldSamples(20, 30)
	o.CleanupOldSamples(25, 35)
	o.CleanupOldSamples(30, 40)
	o.CleanupOldSamples(22, 38)

	//for i, v := range o.samples {
	//	fmt.Printf("i=%d pw=%f edp=%f age=%d\n", i, v.PowerLimit, v.EDP, v.Age)
	//}

	// Because our test config limits the window to 10 samples, and in the last step
	// We clean up 16 values, 6 values are deleted. Thus there should only be 44 samples
	// left in the end.
	if len(o.samples) != 44 {
		t.Fatalf("Deleted the wrong number of samples: remaining samples %d, but expected 44", len(o.samples))
	}
}

func TestWQROptimize(t *testing.T) {
	samples := LoadSamples(t, "testdata/firestarter.bergamo1", 0)

	o, err := NewWQROptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init WQROptimizer: %v", err)
	}

	newLimit, warm1done := o.Start(42.0)
	//fmt.Printf("[ W1] newLimit=%f\n", newLimit)
	newLimit, warm2done := o.Start(ProbeSample(t, samples, newLimit))
	//fmt.Printf("[ W2] newLimit=%f\n", newLimit)
	newLimit, warm3done := o.Start(ProbeSample(t, samples, newLimit))
	//fmt.Printf("[ W3] newLimit=%f\n", newLimit)
	if warm1done || warm2done || !warm3done {
		t.Fatalf("Warmup didn't end in expected 3 steps")
	}

	for i := 0; i < 30; i++ {
		newLimit = o.Update(ProbeSample(t, samples, newLimit))
		//fmt.Printf("[%3d] newLimit=%f\n", i, newLimit)
	}

	if newLimit < 250 || newLimit > 280 {
		t.Fatal("WQR optimizer did not converge correctly")
	}

	//fmt.Printf("========== CHANGE ==========")
	//samples = LoadSamples(t, "testdata/firestarter.genoa1", 0)

	//for i := 0; i < 100 ; i++ {
	//	newLimit = o.Update(ProbeSample(t, samples, newLimit))
	//	fmt.Printf("[%3d] newLimit=%f\n", i, newLimit)
	//}
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
		if len(elements) != 6 {
			continue
		}
		if elements[0] == "SOCKET" {
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
		edp, err := strconv.ParseFloat(strings.TrimSpace(elements[4]), 64)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, SamplePoint{PowerLimit: powerLimit, EDP: edp})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PowerLimit < result[j].PowerLimit
	})

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
		return samples[pos-1].EDP
	} else if pos > 0 {
		l := samples[pos-1]
		r := samples[pos]
		if powerLimit-l.PowerLimit < r.PowerLimit-powerLimit {
			return l.EDP
		} else {
			return r.EDP
		}
	} else {
		return samples[pos].EDP
	}
}
