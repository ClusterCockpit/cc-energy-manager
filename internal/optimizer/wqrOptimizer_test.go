// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	//"fmt"
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
		t.Fatalf("failed to init WQROptimizer: %v", err.Error())
	}
}

func TestWQRInsertSample(t *testing.T) {
	cclog.Init("debug", false)
	o, err := NewWQROptimizer(json.RawMessage(testconfig))
	if err != nil {
		t.Fatalf("failed to init WQROptimizer: %v", err.Error())
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
		t.Fatalf("failed to init WQROptimizer: %v", err.Error())
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
