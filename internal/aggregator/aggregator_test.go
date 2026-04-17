// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"testing"
)

func TestReductionParse(t *testing.T) {
	mode, err := EdpReductionModeParse("harmonicMean")
	if err != nil {
		t.Fatalf("Error occured during conversion: %v", err)
	}
	if mode != EdpReduceHarmMean {
		t.Fatalf("mode parsed incorrectly: %d", int(mode))
	}
}

func TestCalculationArithMean(t *testing.T) {
	edpMap := map[string]map[string]float64{
		"f0601" : map[string]float64{
			"0": 1.0,
			"1": 2.0,
			"2": 3.0,
		},
	}

	m := DeviceEdpToTargetEdp(edpMap, EdpReduceArithMean)

	if v, _ := m[JobScopeTarget()]; v < 1.999 || v > 2.001 {
		t.Errorf("Mean of [0, 1, 2] is not ~2.0: %v", v)
	}
}

func TestCalculationGeomMean(t *testing.T) {
	edpMap := map[string]map[string]float64{
		"f0601" : map[string]float64{
			"0": 1.0,
			"1": 10.0,
			"2": 100.0,
		},
	}

	m := DeviceEdpToTargetEdp(edpMap, EdpReduceGeomMean)

	if v, _ := m[JobScopeTarget()]; v < 9.999 || v > 10.001 {
		t.Errorf("Geom Mean of [1, 10, 100] is not ~10.0: %v", v)
	}
}

func TestCalculationHarmMean(t *testing.T) {
	edpMap := map[string]map[string]float64{
		"f0601" : map[string]float64{
			"0": 1.0,
			"1": 4.0,
			"2": 4.0,
		},
	}

	m := DeviceEdpToTargetEdp(edpMap, EdpReduceHarmMean)

	if v, _ := m[NodeScopeTarget("f0601")]; v < 1.999 || v > 2.001 {
		t.Errorf("Geom Mean of [1, 4, 4] is not ~2.0: %v", v)
	}
}

func TestCalculationMin(t *testing.T) {
	edpMap := map[string]map[string]float64{
		"f0601" : map[string]float64{
			"0": 1.0,
			"1": 2.0,
			"2": 3.0,
		},
	}

	m := DeviceEdpToTargetEdp(edpMap, EdpReduceMin)

	if v, _ := m[JobScopeTarget()]; v != 1.0 {
		t.Errorf("Geom Mean of [1, 2, 3] is not 1.0: %v", v)
	}
}

func TestCalculationMax(t *testing.T) {
	edpMap := map[string]map[string]float64{
		"f0601" : map[string]float64{
			"0": 1.0,
			"1": 2.0,
			"2": 3.0,
		},
	}

	m := DeviceEdpToTargetEdp(edpMap, EdpReduceMax)

	if v, _ := m[JobScopeTarget()]; v != 3.0 {
		t.Errorf("Geom Mean of [1, 2, 3] is not 3.0: %v", v)
	}
}
