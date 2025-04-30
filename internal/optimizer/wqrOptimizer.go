// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"math"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	"github.com/openacid/slimarray/polyfit"
)

// TODO rewrite this text
// This optimizer optimizes power limits based on windowed quadratic regression.
// Optimizing means finding the lowest power usage per instruction rate.
// We try to circumvent the following problems, which other optimizer may face:
// - Noise immunity
// - Prevent unnecessary drift from optimum during normal operation
// - No oscillation
//
// The concept of operation is the following:
// A quadratic regression of the function powerlimit -> edp is calculated.
// Based on that, we find the minimum of this regression. As we obtain more and more
// measurements, the regression will become more accurate.
// Because the true underlying function is not really a positive quadratic function,
// we limit the 'window' in which this quadratic regression operates on.
// This also means that the true minimum may be outside of the window. Should that
// be the case, we simply pretend the minimum is at the respective window border.
// During the next few iterations (with the window moving with the last detected minimum)
// we should hopefully move the window in such a way, that it does cover the area of the
// true minimum. If the window size is choosen okayish, the quadratic minimum in this window
// should be fairly accurate.

type SamplePoint struct {
	PowerLimit float64
	EDP        float64
	Age        int
}

type wqrOptimizer struct {
	lowerBound      float64
	upperBound      float64
	winMinWidth     float64
	winMinSamples   int
	winLimitSamples int
	samples         []SamplePoint
	current         float64
	startupState    int
	rand            *rand.Rand
}

const (
	STARTUP_LOWER int = 0
	STARTUP_UPPER     = 1
	STARTUP_MID       = 2
)

func NewWQROptimizer(config json.RawMessage) (*wqrOptimizer, error) {
	c := struct {
		LowerBound      float64 `json:"lowerBound"`
		UpperBound      float64 `json:"upperBound"`
		WinMinWidth     float64 `json:"winMinWidth"`
		WinMinSamples   int     `json:"winMinSamples"`
		WinLimitSamples int     `json:"winMaxSamples"`
		Deterministic   bool    `json:"deterministic"`
	}{}

	err := json.Unmarshal(config, &c)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse WQR Optimizer: %v", err)
	}

	o := wqrOptimizer{
		lowerBound:      c.LowerBound,
		upperBound:      c.UpperBound,
		winMinWidth:     c.WinMinWidth,
		winMinSamples:   c.WinMinSamples,
		winLimitSamples: c.WinLimitSamples,
	}

	if c.Deterministic {
		o.rand = rand.New(rand.NewSource(42))
	} else {
		o.rand = rand.New(rand.NewSource(rand.Int63()))
	}

	if o.upperBound <= o.lowerBound {
		return nil, fmt.Errorf("WQR optimizer: upperBound (%f) must be higher than lowerBound (%f)", o.upperBound, o.lowerBound)
	}

	if o.lowerBound <= 0.0 {
		return nil, fmt.Errorf("WQR optimizer: lowerBound (%f) must be > 0.0", o.lowerBound)
	}

	o.winMinSamples = max(3, o.winMinSamples)
	if o.winMinWidth <= 0.0 {
		o.winMinWidth = (o.upperBound - o.lowerBound) / 10.0
	}
	o.winLimitSamples = max(10, o.winMinSamples)

	o.startupState = STARTUP_LOWER

	return &o, nil
}

func (o *wqrOptimizer) Start(edp float64) (float64, bool) {
	// Perform an initial test regression at the lower boundary, upper boundary,
	// and inbetween. After that, we can go over to normal operation and perform
	// a quadratic regression.
	if o.startupState == STARTUP_LOWER {
		// No o.InsertSample() is performed here, since the initial measurement is not usable.
		o.current = o.lowerBound
		o.startupState = STARTUP_UPPER
		return o.lowerBound, false
	} else if o.startupState == STARTUP_UPPER {
		o.InsertSample(o.current, edp)
		o.current = o.upperBound
		o.startupState = STARTUP_MID
		return o.upperBound, false
	} else if o.startupState == STARTUP_MID {
		o.InsertSample(o.current, edp)
		o.current = o.lowerBound + 0.5*(o.upperBound-o.lowerBound)
		return o.current, true
	}

	cclog.Fatalf("Start() must not be called once it has returned true")
	return 0.0, false
}

func (o *wqrOptimizer) Update(edp float64) float64 {
	pos := o.InsertSample(o.current, edp)

	// Search for neighbouring samples to our current observed samples until:
	// - winMinSamples is reached and minMinWidth is reached (latter only if enough samples are available)
	winLeftIndex := pos      // inclusive
	winRightIndex := pos + 1 // exclusive

	var winLeftPowerLimit float64
	var winRightPowerLimit float64

	for {
		winLeftPowerLimit = o.samples[winLeftIndex].PowerLimit
		winRightPowerLimit = o.samples[winRightIndex-1].PowerLimit

		enoughSamples := winRightIndex-winLeftIndex >= o.winMinSamples
		enoughWidth := winRightPowerLimit-winLeftPowerLimit >= o.winMinWidth

		if enoughSamples && enoughWidth {
			break
		}

		if winLeftIndex <= 0 && winRightIndex >= len(o.samples) {
			break
		}

		if winLeftIndex <= 0 {
			// If the leftmost index has already been searched, only search to the right.
			winRightIndex += 1
		} else if winRightIndex >= len(o.samples) {
			// If the rightmost index has already been searched, only search to the left.
			winLeftIndex -= 1
		} else {
			// Search into the closest direction
			distanceLeft := o.current - o.samples[winLeftIndex-1].PowerLimit
			distanceRight := o.samples[winRightIndex].PowerLimit - o.current
			if distanceLeft < distanceRight {
				winLeftIndex -= 1
			} else {
				winRightIndex += 1
			}
		}
	}

	x := make([]float64, winRightIndex-winLeftIndex)
	y := make([]float64, winRightIndex-winLeftIndex)

	for i := 0; i < len(x); i++ {
		x[i] = o.samples[winLeftIndex+i].PowerLimit
		y[i] = o.samples[winLeftIndex+i].EDP
		//fmt.Printf("[x=%f y=%f] ", x[i], y[i])
	}
	//fmt.Println("")

	coefficients := polyfit.NewFit(x, y, 2).Solve()
	// ax^2 + bx + c
	a := coefficients[2]
	b := coefficients[1]
	c := coefficients[0]//, c is irrelevant for finding a minimum

	// If 'a' is positive, our regressed quadratic function has a global minimum.
	// If 'a' is negative, our regressed quadratic function has a global maximum.
	if math.IsNaN(a) || math.IsInf(a, 0) || math.IsNaN(b) || math.IsInf(b, 0) {
		// Not sure if the polyfit library is supposed to do that, but it does.
		// I assume this may occur due to numerical instability. Just move around
		// randomly a bit in this case.
		o.current += o.rand.Float64() * 0.1 * (o.upperBound - o.lowerBound)
	} else if a <= 0.0 {
		// If a is negative, we can't search a minimum. Instead, we use the slope 'b'
		// to determine in which direction to search.
		randomize := 0.2
		if b > 0.0 {
			o.current = o.current - 0.5*(o.current-winLeftPowerLimit)
			randomize = 0.05
		} else if b < 0.0 {
			o.current = o.current + 0.5*(winRightPowerLimit-o.current)
			randomize = 0.05
		}
		o.current += o.rand.Float64() * randomize * (o.upperBound - o.lowerBound)
	} else {
		// for positive 'a', the minimum of ax^2 + bx + c is the following:
		// min(ax^2 + bx + c) = solve(2ax + b == 0)
		//   = solve(2ax == -b) = solve(x == -b/2a) = -b/2a
		// If the result is outside of the bounds, limit it to the bounds.
		// In that case, apply a random bounce effect. Hopefully this avoids deadlocks are the
		// borders.

		// TODO we might want to add a smoothing factor here, in order to reduce oscillation
		// at the true minimum.
		o.current = -b / (2.0 * a)
		o.current += (0.05*o.rand.Float64() - 0.025) * (o.upperBound-o.lowerBound)
	}

	if o.current < o.lowerBound {
		o.current = max(o.current, o.lowerBound) + 0.025*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}
	if o.current > o.upperBound {
		o.current = min(o.current, o.upperBound) - 0.025*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}

	//nx := fmt.Sprintf("%f", o.samples[winLeftIndex].PowerLimit)
	//ny := fmt.Sprintf("%f", o.samples[winLeftIndex].EDP)
	//for i := winLeftIndex + 1; i < winRightIndex; i++ {
	//	nx = fmt.Sprintf("%s,%f", nx, o.samples[i].PowerLimit)
	//	ny = fmt.Sprintf("%s,%f", ny, o.samples[i].EDP)
	//}
	//fmt.Printf("%.12f;%.12f;%.12f;%f;%f;%f;%d;%s;%s\n", a, b, c, winLeftPowerLimit, winRightPowerLimit, o.current, winRightIndex - winLeftIndex, nx, ny)
	_ = c

	o.CleanupOldSamples(winLeftIndex, winRightIndex)
	return o.current
}

func (o *wqrOptimizer) InsertSample(powerLimit, edp float64) int {
	cmpFunc := func(s SamplePoint, t float64) int {
		if s.PowerLimit < t {
			return -1
		}
		if s.PowerLimit > t {
			return 1
		}
		return 0
	}

	pos, _ := slices.BinarySearchFunc(o.samples, powerLimit, cmpFunc)
	o.samples = slices.Insert(o.samples, pos, SamplePoint{PowerLimit: powerLimit, EDP: edp})
	return pos
}

// TODO, we should probably rewrite thisand discard, somewhat randomly, samples
// which are closest to each other.

func (o *wqrOptimizer) CleanupOldSamples(leftIndex, rightIndex int) {
	// Reset age of samples outside of current window, increment it for all the ones in our window.
	// The idea is that we will converge in an area somewhere. In this area, we do not want
	// to accumulate infinitely old values, so we discard them. The ones outside we will keep
	// in order to at least have a rought idea where previous performance was. Should the window
	// move back to the area, which was previously not part of our window, those should get cycled
	// at some point as well.
	for i := 0; i < leftIndex; i++ {
		o.samples[i].Age = max(0, o.samples[i].Age-1)
	}
	for i := leftIndex; i < rightIndex; i++ {
		o.samples[i].Age += 1
	}
	for i := rightIndex; i < len(o.samples); i++ {
		o.samples[i].Age = max(0, o.samples[i].Age-1)
	}

	// Now we limit the amount of samples inside the window (between leftIndex and rightIndex) to count of winLimitSamples.
	// We prioritize removal of the oldest values.
	indicesToRemove := make([]int, rightIndex-leftIndex)
	if len(indicesToRemove) >= o.winLimitSamples {
		for i := 0; i < len(indicesToRemove); i++ {
			indicesToRemove[i] = i + leftIndex
		}

		// We randommize the list before looking sorting by oldest age. That way samples
		// with equal age are removed in a random order.
		o.rand.Shuffle(len(indicesToRemove), func(i, j int) {
			indicesToRemove[i], indicesToRemove[j] = indicesToRemove[j], indicesToRemove[i]
		})

		sort.Slice(indicesToRemove, func(i, j int) bool {
			return o.samples[indicesToRemove[i]].Age < o.samples[indicesToRemove[j]].Age
		})

		indicesToRemove = indicesToRemove[o.winLimitSamples:]
		o.DeleteSamplesAtIndices(leftIndex, rightIndex, indicesToRemove)
	}
}

func (o *wqrOptimizer) DeleteSamplesAtIndices(leftIndex, rightIndex int, indicesToRemove []int) {
	sort.Ints(indicesToRemove)
	// leftIndex and rightIndex are merely an optimization so that we do not iterate over
	// unnecessary values.
	leftSamples := o.samples[0:leftIndex]
	windowSamples := o.samples[leftIndex:rightIndex]
	rightSamples := o.samples[rightIndex:len(o.samples)]

	indexIndex := 0
	writeIndex := 0
	for readIndex := 0; readIndex < len(windowSamples); readIndex++ {
		if indexIndex < len(indicesToRemove) && leftIndex+readIndex == indicesToRemove[indexIndex] {
			indexIndex += 1
		} else {
			o.samples[writeIndex] = o.samples[readIndex]
			writeIndex += 1
		}
	}

	o.samples = append(leftSamples, windowSamples[0:writeIndex]...)
	o.samples = append(o.samples, rightSamples...)
}
