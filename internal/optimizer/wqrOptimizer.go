// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	"gonum.org/v1/gonum/mat"
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
// A quadratic regression of the function powerlimit -> pdp is calculated.
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
	PDP        float64
}

type ds struct {
	distance float64
	index int
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

func (o *wqrOptimizer) Start(pdp float64) (float64, bool) {
	// Perform an initial test regression at the lower boundary, upper boundary,
	// and inbetween. After that, we can go over to normal operation and perform
	// a quadratic regression.
	if o.startupState == STARTUP_LOWER {
		// No o.InsertSample() is performed here, since the initial measurement is not usable.
		o.current = o.lowerBound
		o.startupState = STARTUP_UPPER
		return o.lowerBound, false
	} else if o.startupState == STARTUP_UPPER {
		o.InsertSample(o.current, pdp)
		o.current = o.upperBound
		o.startupState = STARTUP_MID
		return o.upperBound, false
	} else if o.startupState == STARTUP_MID {
		o.InsertSample(o.current, pdp)
		o.current = o.lowerBound + 0.5*(o.upperBound-o.lowerBound)
		return o.current, true
	}

	cclog.Fatalf("Start() must not be called once it has returned true")
	return 0.0, false
}

func (o *wqrOptimizer) Update(pdp float64) float64 {
	pos := o.InsertSample(o.current, pdp)

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
		enoughWidth := winRightPowerLimit - winLeftPowerLimit >= o.winMinWidth

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
		y[i] = o.samples[winLeftIndex+i].PDP
		//fmt.Printf("[x=%f y=%f] ", x[i], y[i])
	}
	//fmt.Println("")

	coefficients, err := PolyFit(x, y, 2)
	// ax^2 + bx + c
	a := coefficients[2]
	b := coefficients[1]
	c := coefficients[0]

	// Randomly move a bit into the direction where there is the most space between samples.
	// This hopefully avoids big gaps in the regression, which otherwise cause inaccuracies.
	distances := o.GetSampleDistances()
	distances = distances[winLeftIndex:winRightIndex]
	SortSampleDistanceDescending(distances)

	distanceImbalanceCorrection := 0.0
	if distances[0].distance > distances[len(distances)-1].distance * 3 {
		distanceImbalanceCorrection += (0.5*o.rand.Float64()) * (o.samples[distances[0].index].PowerLimit - o.current)
	}

	o.CleanupOldSamples(winLeftIndex, winRightIndex)

	// If 'a' is positive, our regressed quadratic function has a global minimum.
	// If 'a' is negative, our regressed quadratic function has a global maximum.
	if err != nil || math.IsNaN(a) || math.IsInf(a, 0) || math.IsNaN(b) || math.IsInf(b, 0) {
		// Not sure if the polyfit library is supposed to do that, but it does.
		// I assume this may occur due to numerical instability. Just move around
		// randomly a bit in this case.
		o.current += (o.rand.Float64() * 0.1 - 0.05) * (o.upperBound - o.lowerBound)
	} else if a <= 0.0 {
		// If a is negative, we can't search a minimum. Instead, we use the slope
		// between the window borders to determine a search direction.
		yl := a * (winLeftPowerLimit * winLeftPowerLimit) + b * winLeftPowerLimit + c
		yr := a * (winRightPowerLimit * winRightPowerLimit) + b * winRightPowerLimit + c
		randomize := 0.2
		if yl < yr {
			o.current = o.current - 0.5*(o.current-winLeftPowerLimit)
			randomize = 0.05
		} else if b < 0.0 {
			o.current = o.current + 0.5*(winRightPowerLimit-o.current)
			randomize = 0.05
		}
		o.current += (o.rand.Float64() - 0.5) * randomize * (o.upperBound - o.lowerBound)
	} else {
		// for positive 'a', the minimum of ax^2 + bx + c is the following:
		// min(ax^2 + bx + c) = solve(2ax + b == 0)
		//   = solve(2ax == -b) = solve(x == -b/2a) = -b/2a
		o.current = -b / (2.0 * a)
		o.current += (0.1*o.rand.Float64() - 0.05) * (winRightPowerLimit - winLeftPowerLimit)
		o.current += distanceImbalanceCorrection
	}

	// If the result is outside of the bounds, limit it to the bounds.
	// In that case, apply a random bounce effect. Hopefully this avoids deadlocks are the
	// borders.
	if o.current < o.lowerBound {
		o.current = max(o.current, o.lowerBound) + 0.025*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}
	if o.current > o.upperBound {
		o.current = min(o.current, o.upperBound) - 0.025*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}

	//nx := fmt.Sprintf("%f", x[0])
	//ny := fmt.Sprintf("%f", y[0])
	//for i := 1; i < len(x); i++ {
	//	nx = fmt.Sprintf("%s,%f", nx, x[i])
	//	ny = fmt.Sprintf("%s,%f", ny, y[i])
	//}
	//fmt.Printf("%.12f;%.12f;%.12f;%f;%f;%f;%d;%s;%s\n", a, b, c, winLeftPowerLimit, winRightPowerLimit, o.current, winRightIndex - winLeftIndex, nx, ny)

	return o.current
}

func (o *wqrOptimizer) InsertSample(powerLimit, pdp float64) int {
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
	o.samples = slices.Insert(o.samples, pos, SamplePoint{PowerLimit: powerLimit, PDP: pdp})
	return pos
}

func (o *wqrOptimizer) CleanupOldSamples(leftIndex, rightIndex int) {
	distances := o.GetSampleDistances()
	//fmt.Printf("a distances: %+v\n", distances)

	distances = distances[leftIndex:rightIndex]
	if len(distances) < o.winLimitSamples {
		return
	}
	//fmt.Printf("b distances: %+v\n", distances)
	SortSampleDistanceDescending(distances)
	//fmt.Printf("c distances: %+v\n", distances)
	distances = distances[o.winLimitSamples:]
	indicesToRemove := make([]int, len(distances))
	for i := 0; i < len(indicesToRemove); i++ {
		indicesToRemove[i] = distances[i].index
	}

	//fmt.Printf("d distances: %+v\n", distances)

	// exclude values outside the desired window from deletion
	indicesToRemove = slices.DeleteFunc(indicesToRemove, func(index int) bool {
		return math.Abs(o.samples[index].PowerLimit - o.current) > o.winMinWidth / 2
	})

	//fmt.Printf("e distances: %+v\n", distances)

	o.DeleteSamplesAtIndices(indicesToRemove)
}

func (o *wqrOptimizer) DeleteSamplesAtIndices(indicesToRemove []int) {
	sort.Ints(indicesToRemove)
	//fmt.Printf("           -> to remove: %v\n", indicesToRemove)
	//fmt.Printf("samples before: %+v\n", o.samples)

	indexIndex := 0
	writeIndex := 0
	for readIndex := 0; readIndex < len(o.samples); readIndex++ {
		if indexIndex < len(indicesToRemove) && readIndex == indicesToRemove[indexIndex] {
			indexIndex += 1
		} else {
			o.samples[writeIndex] = o.samples[readIndex]
			writeIndex += 1
		}
	}

	o.samples = o.samples[0:writeIndex]
	//fmt.Printf("samples after: %+v\n", o.samples)
}

func PolyFit(x, y []float64, degree int) ([]float64, error) {
	// Based on this example:
	// https://github.com/gonum/gonum/issues/1759#issuecomment-1005668867
	d := degree + 1
	a := mat.NewDense(len(x), d, nil)
	for i := range x {
		for j, p := 0, 1.; j < d; j, p = j+1, p*x[i] {
			a.Set(i, j, p)
		}
	}
	b := mat.NewDense(len(y), 1, y)
	c := mat.NewDense(d, 1, nil)


	var qr mat.QR
	qr.Factorize(a)

	retval := make([]float64, d)
	err := qr.SolveTo(c, false, b)
	if err != nil {
		return retval, fmt.Errorf("Unable to fit curve: %w", err)
	}

	for i := 0; i < d; i++ {
		retval[i] = c.At(i, 0)
	}
	return retval, nil
}

func (o *wqrOptimizer) GetSampleDistances() []ds {
	distances := make([]ds, len(o.samples))
	for i := 0; i < len(distances); i++ {
		var dl float64
		if i <= 0 {
			dl = 100000
		} else {
			dl = o.samples[i].PowerLimit - o.samples[i-1].PowerLimit
		}

		var dr float64
		if i >= len(distances)-1 {
			dr = 100000
		} else {
			dr = o.samples[i+1].PowerLimit - o.samples[i].PowerLimit
		}

		distances[i].distance = dl + dr + 0.01 * (o.rand.Float64() - 0.5) * (o.upperBound - o.lowerBound)
		distances[i].index = i
	}
	return distances
}

func SortSampleDistanceDescending(distances []ds) {
	slices.SortFunc(distances, func(a, b ds) int {
		return cmp.Compare(b.distance, a.distance)
	})
}
