// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"math/rand"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

type ProbeMode int

// enum optimizing strategy
const (
	ProbeLowerOuter ProbeMode = iota
	ProbeLowerInner
	ProbeUpperInner
	ProbeUpperOuter
	ProbeNone
)

type sample struct {
	// age        int //TODO discard sample y if the sample is too old
	x float64
	y float64
}

type gssngOptimizer struct {
	lowerOuter   sample
	lowerInner   sample
	upperInner   sample
	upperOuter   sample
	probeMode    ProbeMode
	widthMin     float64
	retriesMax   int
	retriesCount int
	borderLower  float64
	borderUpper  float64
	borderLowerCfg float64
	borderUpperCfg float64
	validSampleMin float64
	validSampleMax float64
	fudgeFactor  float64
}

func (o *gssngOptimizer) Start(y float64) (float64, bool) {
	o.probeMode = ProbeLowerOuter
	return o.lowerOuter.x, true
}

func (o *gssngOptimizer) Update(y float64) float64 {
	y = max(0.0, y)

	o.DumpState("before")
	defer o.DumpState("after")

	// check sample order consistency
	o.CheckConsistency()

	// save last measured value
	if o.probeMode == ProbeLowerOuter {
		o.lowerOuter.y = y
	} else if o.probeMode == ProbeLowerInner {
		o.lowerInner.y = y
	} else if o.probeMode == ProbeUpperInner {
		o.upperInner.y = y
	} else if o.probeMode == ProbeUpperOuter {
		o.upperOuter.y = y
	}

	// perform the actual current operation
	if !o.TryNarrow() {
		if !o.TryBroaden() {
			cclog.ComponentWarn("Unable to find minimum of function. Is there any significant load on the target nodes?")
		}
	}

	// probe remaining missing values
	if o.lowerOuter.y < 0 {
		o.probeMode = ProbeLowerOuter
		return o.lowerOuter.x
	}

	if o.lowerInner.y < 0 {
		o.probeMode = ProbeLowerInner
		return o.lowerInner.x
	}

	if o.upperInner.y < 0 {
		o.probeMode = ProbeUpperInner
		return o.upperInner.x
	}

	if o.upperOuter.y < 0 {
		o.probeMode = ProbeUpperOuter
		return o.upperOuter.x
	}

	cclog.ComponentError("gssng", "No values to update")
	switch rand.Int31n(4) {
	case 0:
		o.probeMode = ProbeLowerOuter
		return o.lowerOuter.x
	case 1:
		o.probeMode = ProbeLowerInner
		return o.lowerInner.x
	case 2:
		o.probeMode = ProbeUpperInner
		return o.upperInner.x
	default:
		o.probeMode = ProbeUpperOuter
		return o.upperOuter.x
	}
}

func (o *gssngOptimizer) GetBordersCfg() (float64, float64) {
	return o.borderLowerCfg, o.borderUpperCfg
}

func (o *gssngOptimizer) GetBordersCur() (float64, float64) {
	return o.borderLower, o.borderUpper
}

func (o *gssngOptimizer) SetBorders(lower, upper float64) {
	o.borderLower = max(lower, o.borderLowerCfg)
	o.borderUpper = min(upper, o.borderUpperCfg)

	o.ClampToBorders()
	
	// Set probe mode, since this function may be called, while a measurement is currently in progress.
	// Once we run Update again, we will have to restart the measurement in order to actually obtain
	// the correct value.
	o.probeMode = ProbeNone
}

func (o *gssngOptimizer) CheckConsistency() {
	// If we coded everything correctly, this check should never fail.
	// Hence why we Fatal in case of failure.
	if o.lowerOuter.x >= o.lowerInner.x || o.lowerInner.x >= o.upperInner.x || o.upperInner.x >= o.upperOuter.x {
		cclog.Fatalf("GSSNG Bad invariant. Sample order condition not correct: [%f < %f < %f < %f]",
			o.lowerOuter.x, o.lowerInner.x, o.upperInner.x, o.upperOuter.x)
	}
}

func (o *gssngOptimizer) DumpState(position string) {
	cclog.Debugf("State at %s", position)
	cclog.Debugf("\tf_loOut(%f): %f", o.lowerOuter.x, o.lowerOuter.y)
	cclog.Debugf("\tf_loInn(%f): %f", o.lowerInner.x, o.lowerInner.y)
	cclog.Debugf("\tf_hiInn(%f): %f", o.upperInner.x, o.upperInner.y)
	cclog.Debugf("\tf_hiOut(%f): %f", o.upperOuter.x, o.upperOuter.y)
	cclog.Debugf("\tretriesCount: %d", o.retriesCount)
	cclog.Debugf("\tprobeMode: %d", int(o.probeMode))
}

func (o *gssngOptimizer) TryNarrow() bool {
	// This function shall returns false if narrowing is not possible, and broaden should
	// be attempted instead.

	// If samples a sample is valid but out of range, fail. This allows to quickly broaden
	// in case there is not enough load on the machine.
	if !o.AllSamplesValidRange() {
		return false
	}

	// do we have all required values? If not, delay narrow until they are all there
	if !o.AllSamplesValid() {
		return true
	}

	// Detect whether we can narrow our GSS or not.
	// In order to do so, y curve must be unimodal.
	if !o.IsUnimodal() {
		// We cannot continue with GSS at this point, because a gss assumes an unimodal function.
		// Instead, we first try to retry measuring the inner two values.
		// If that doesn't change the situation, we instead broaden later after a couple of retries.
		if o.retriesCount >= o.retriesMax {
			o.retriesCount = 0
			return false
		} else {
			o.retriesCount += 1
			o.InvalidateAll()
			return true
		}
	}

	o.retriesCount = 0

	// are we allowed to narrow further?
	if (o.upperOuter.x - o.lowerOuter.x) * invphi < o.widthMin {
		return false
	}

	// perform the actual narrow
	if o.lowerInner.y < o.upperInner.y {
		o.NarrowDown()
	} else {
		o.NarrowUp()
	}

	return true
}

func (o *gssngOptimizer) TryBroaden() bool {
	// do we have all required values? If not, delay narrow until they are all there
	if !o.AllSamplesValid() {
		return true
	}

	// broaden into the direction where there is space
	if o.lowerOuter.x == o.borderLower && o.upperOuter.x == o.borderUpper {
		// We cannot broaden further, both borders reached.
		// Reset all values in order to obtain new ones, because at some point
		// out function may become solvable again.
		o.InvalidateAll()
		return false
	} else if o.lowerOuter.x == o.borderLower && o.upperOuter.x < o.borderUpper {
		o.BroadenUp()
	} else if o.upperOuter.x == o.borderUpper && o.lowerOuter.x > o.borderLower {
		o.BroadenDown()
	} else if o.lowerOuter.y < o.upperOuter.y {
		o.BroadenDown()
	} else {
		o.BroadenUp()
	}

	// Check if we have exceeded our borders
	if o.lowerOuter.x < o.borderLower && o.upperOuter.x > o.borderUpper {
		// both borders exceeded, clamp all samples to borders and invalidate them all
		o.ClampToBorders()
	} else if o.lowerOuter.x < o.borderLower {
		// only lower border exceeded, reset all except upper outer sample
		o.ClampToLowerBorder()
	} else if o.upperOuter.x > o.borderUpper {
		// only upper border exceeded, reset all except lower outer sample
		o.ClampToUpperBorder()
	}

	return true
}

func (o *gssngOptimizer) BroadenUp() {
	cclog.Debugf("BroadenUp")
	// |---|--|---|
	// becomes
	// |------|---|------|
	o.lowerInner = o.upperInner
	o.upperInner = o.upperOuter
	o.upperOuter.x = o.lowerOuter.x + phi * (o.upperOuter.x - o.lowerOuter.x)
	o.upperOuter.y = -1
}

func (o *gssngOptimizer) BroadenDown() {
	cclog.Debugf("BroadenDown")
	//        |---|--|---|
	// becomes
	// |------|---|------|
	o.upperInner = o.lowerInner
	o.lowerInner = o.lowerOuter
	o.lowerOuter.x = o.upperOuter.x - phi * (o.upperOuter.x - o.lowerOuter.x)
	o.lowerOuter.y = -1
}

func (o *gssngOptimizer) NarrowUp() {
	cclog.Debugf("NarrowUp")
	// |------|---|------|
	// becomes
	//        |---|--|---|
	o.lowerOuter = o.lowerInner
	o.lowerInner = o.upperInner
	o.upperInner.x = o.lowerOuter.x + invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.upperInner.y = -1
}

func (o *gssngOptimizer) NarrowDown() {
	cclog.Debugf("NarrowDown")
	// |------|---|------|
	// becomes
	// |---|--|---|
	o.upperOuter = o.upperInner
	o.upperInner = o.lowerInner
	o.lowerInner.x = o.upperOuter.x - invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.lowerInner.y = -1
}

func (o *gssngOptimizer) ClampToBorders() {
	o.lowerOuter.x = o.borderLower
	o.upperOuter.x = o.borderUpper
	o.lowerInner.x = o.upperOuter.x - invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.upperInner.x = o.lowerOuter.x + invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.InvalidateAll()
}

func (o *gssngOptimizer) ClampToLowerBorder() {
	o.lowerOuter.x = o.borderLower
	o.lowerOuter.y = -1
	o.lowerInner.x = o.upperOuter.x - invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.lowerInner.y = -1
	o.upperInner.x = o.lowerOuter.x + invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.upperInner.y = -1
}

func (o *gssngOptimizer) ClampToUpperBorder() {
	o.upperOuter.x = o.borderUpper
	o.upperOuter.y = -1
	o.lowerInner.x = o.upperOuter.x - invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.lowerInner.y = -1
	o.upperInner.x = o.lowerOuter.x + invphi * (o.upperOuter.x - o.lowerOuter.x)
	o.upperInner.y = -1
}

func (o *gssngOptimizer) InvalidateInner() {
	o.lowerInner.y = -1
	o.upperInner.y = -1
}

func (o *gssngOptimizer) InvalidateAll() {
	o.lowerOuter.y = -1
	o.lowerInner.y = -1
	o.upperInner.y = -1
	o.upperOuter.y = -1
}

func (o *gssngOptimizer) IsUnimodal() bool {
	// If below checks, whether inner samples are actually smaller than outer samples.
	// This is a mandatory requirement for GSS to be a applied to assert it's actually
	// unimodal:
	// loOut < loInn < hiInn < hiOut: ok
	// loOut > loInn < hiInn < hiOut: ok
	// loOut > loInn > hiInn < hiOut: ok
	// loOut > loInn > hiInn > hiOut: ok
	// everything else: not ok
	if o.lowerOuter.y < o.lowerInner.y {
		if o.lowerInner.y < o.upperInner.y && o.upperInner.y < o.upperInner.y && o.upperInner.y < o.upperOuter.y {
			return true
		}
	} else if o.lowerInner.y < o.upperInner.y {
		if o.upperInner.y < o.upperOuter.y {
			return true
		}
	} else {
		// here it doesn't matter if upperInner is smaller or larger than upperOuter
		return true
	}

	return false
}

func (o *gssngOptimizer) AllSamplesValidRange() bool {
	// If samples are not in the configured range, we reject them.
	// This prevents the case (e.g. during idle loads), where extreme samples are observed.
	// Those are then forcefully rejected and cause a Broaden.
	samples := []float64{o.lowerOuter.y, o.lowerInner.y, o.upperInner.y, o.upperOuter.y}
	for _, s := range samples {
		if s > 0.0 && (s < o.validSampleMin || s > o.validSampleMax) {
			return false
		}
	}

	return true
}

func (o *gssngOptimizer) AllSamplesValid() bool {
	if o.lowerOuter.y <= 0.0 || o.lowerInner.y <= 0.0 || o.upperInner.y <= 0.0 || o.upperOuter.y < 0.0 {
		return false
	}
	return true
}

func NewGssNgOptimizer(config json.RawMessage) (*gssngOptimizer, error) {
	var c struct {
		Tolerance float64 `json:"tolerance"`
		Borders   struct {
			Lower float64 `json:"lower"`
			Upper float64 `json:"upper"`
		} `json:"borders,omitempty"`
		ValidSampleMin float64 `json:"validSampleMin"`
		ValidSampleMax float64 `json:"validSampleMax"`
		FudgeFactor  float64 `json:"fudgeFactor"`
		RetriesMax int `json:"retriesMax"`
	}

	c.Tolerance = 10
	c.Borders.Lower = 50
	c.Borders.Upper = 500
	c.RetriesMax = 3
	c.FudgeFactor = 0.0
	c.ValidSampleMin = 0
	c.ValidSampleMax = 1000000000

	err := json.Unmarshal(config, &c)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError("GSS", err.Error())
		return nil, err
	}

	o := gssngOptimizer{
		lowerOuter: sample{
			x: c.Borders.Lower,
			y: -1,
		},
		lowerInner: sample{
			x: c.Borders.Upper - invphi * (c.Borders.Upper - c.Borders.Lower),
			y: -1,
		},
		upperInner: sample{
			x: c.Borders.Lower + invphi * (c.Borders.Upper - c.Borders.Lower),
			y: -1,
		},
		upperOuter: sample{
			x: c.Borders.Upper,
			y: -1,
		},
		probeMode:   ProbeNone,
		widthMin:    c.Tolerance,
		retriesMax:  c.RetriesMax,
		retriesCount: 0,
		borderLower: c.Borders.Lower,
		borderUpper: c.Borders.Upper,
		validSampleMin: c.ValidSampleMin,
		validSampleMax: c.ValidSampleMax,
		fudgeFactor: c.FudgeFactor,
	}

	return &o, err
}
