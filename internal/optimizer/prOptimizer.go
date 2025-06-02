// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	"github.com/mihonen/polynomials"
)

type prOptimizer struct {
	lowerBound          float64
	upperBound          float64
	maxPolynomialDegree int
	maxSamples          int
	samples             []SamplePoint
	current             float64
	startupState        int
	rand                *rand.Rand
	enableDebug         bool
	sampleHalfLifeProb  float64
}

func NewPROptimizer(config json.RawMessage) (*prOptimizer, error) {
	c := struct {
		LowerBound          float64 `json:"lowerBound"`
		UpperBound          float64 `json:"upperBound"`
		MaxPolynomialDegree int     `json:"maxPolynomialDegree"`
		MaxSamples          int     `json:"maxSamples"`
		SampleHalfLifeIters int     `json:"sampleHalfLifeIter"`
		Deterministic       bool    `json:"deterministic"`
		EnableDebug         bool    `json:"enableDebug"`
	}{}

	err := json.Unmarshal(config, &c)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse PR Optimizer: %v", err)
	}

	o := prOptimizer{
		lowerBound:          c.LowerBound,
		upperBound:          c.UpperBound,
		maxPolynomialDegree: c.MaxPolynomialDegree,
		maxSamples:          c.MaxSamples,
		enableDebug:         c.EnableDebug,
	}

	if c.Deterministic {
		o.rand = rand.New(rand.NewSource(42))
	} else {
		o.rand = rand.New(rand.NewSource(rand.Int63()))
	}

	if c.SampleHalfLifeIters > 1 {
		o.sampleHalfLifeProb = math.Pow(2, -1.0/float64(c.SampleHalfLifeIters))
	} else {
		o.sampleHalfLifeProb = 0.5
	}

	if o.maxSamples < 5 {
		return nil, fmt.Errorf("PR optimizer: maxSamples (%d) must be at least 5")
	}

	if o.maxPolynomialDegree < 2 {
		return nil, fmt.Errorf("PR optimizer: maxPolynomialDegree (%d) must be at least 2", o.maxPolynomialDegree)
	}

	if o.maxPolynomialDegree > 10 {
		return nil, fmt.Errorf("PR optimizer: maxPolynomialDegree (%d) must not be higher than 10", o.maxPolynomialDegree)
	}

	if o.upperBound <= o.lowerBound {
		return nil, fmt.Errorf("PR optimizer: upperBound (%f) must be higher than lowerBound (%f)", o.upperBound, o.lowerBound)
	}

	if o.lowerBound <= 0.0 {
		return nil, fmt.Errorf("PR optimizer: lowerBound (%f) must be > 0.0", o.lowerBound)
	}

	o.startupState = STARTUP_LOWER

	return &o, nil
}

func (o *prOptimizer) Start(pdp float64) (float64, bool) {
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

func (o *prOptimizer) Update(pdp float64) float64 {
	_ = o.InsertSample(o.current, pdp)
	debugNote := ""

	x := make([]float64, len(o.samples))
	y := make([]float64, len(o.samples))

	for i := 0; i < len(x); i++ {
		x[i] = o.samples[i].PowerLimit
		y[i] = o.samples[i].PDP
	}

	fmt.Printf("sample count: %v\n", len(o.samples))

	polynomialDegree := min(len(o.samples)-1, o.maxPolynomialDegree)
	coefficients, err := PolyFit(x, y, polynomialDegree)

	var minimum float64
	if err == nil {
		minimum, err = o.GetMinimum(coefficients)
	}

	distances := o.GetSampleDistances()
	SortSampleDistanceDescending(distances)

	distanceImbalanceCorrection := 0.0
	for _, dist := range distances {
		powerLimit := o.samples[dist.index].PowerLimit
		if dist.index-1 < 0 || dist.index+1 >= len(o.samples) {
			continue
		}
		powerLimitPrev := o.samples[dist.index-1].PowerLimit
		powerLimitNext := o.samples[dist.index+1].PowerLimit
		dPrev := powerLimit - powerLimitPrev
		dNext := powerLimitNext - powerLimit

		if dPrev >= dNext*2.0 {
			distanceImbalanceCorrection += (1.00 * o.rand.Float64()) * (o.samples[dist.index-1].PowerLimit - o.current)
			break
		} else if dPrev*2.0 <= dNext {
			distanceImbalanceCorrection += (1.00 * o.rand.Float64()) * (o.samples[dist.index+1].PowerLimit - o.current)
			break
		}
	}

	if math.Abs(distanceImbalanceCorrection) > 0.0 {
		debugNote += fmt.Sprintf(" imbalance corrected (%f)", distanceImbalanceCorrection)
	}

	o.CleanupOldSamples()

	if err != nil {
		// Cannot determine minimum (possibly measured values are too flat.
		// Just randomly walk around
		o.current += (o.rand.Float64()*0.1 - 0.05) * (o.upperBound - o.lowerBound)
	} else {
		o.current = minimum
		o.current += distanceImbalanceCorrection
		o.current += (0.025*o.rand.Float64() - 0.0125) * (o.upperBound - o.lowerBound)
	}

	if o.current <= o.lowerBound {
		o.current = max(o.current, o.lowerBound) + 0.05*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}
	if o.current >= o.upperBound {
		o.current = min(o.current, o.upperBound) - 0.05*o.rand.Float64()*(o.upperBound-o.lowerBound)
	}

	if o.enableDebug {
		coefficientsStr := fmt.Sprintf("%.15f", coefficients[0])
		for i := 1; i < len(coefficients); i++ {
			coefficientsStr = fmt.Sprintf("%s:%.15f", coefficientsStr, coefficients[i])
		}

		xStr := fmt.Sprintf("%.15f", x[0])
		yStr := fmt.Sprintf("%.15f", y[0])

		for i := 1; i < len(x); i++ {
			xStr = fmt.Sprintf("%s:%.15f", xStr, x[i])
			yStr = fmt.Sprintf("%s:%.15f", yStr, y[i])
		}

		fmt.Printf("%f;%f;%s;%s;%s\n", minimum, o.current, coefficientsStr, xStr, yStr)
	}

	return o.current
}

func (o *prOptimizer) GetMinimum(polynomialCoefficients []float64) (float64, error) {
	// Our parameter has the coefficients in ascending degree order.
	// The polynomials package needs them to be in descending order
	polynomialCoefficients = slices.Clone(polynomialCoefficients)
	slices.Reverse(polynomialCoefficients)
	poly := polynomials.CreatePolynomial(polynomialCoefficients...)
	polyDerivFirst := poly.Derivative()
	polyDerivSecond := polyDerivFirst.Derivative()

	//fmt.Printf("poly: %+v\n", poly)
	//fmt.Printf("polyDerivFirst: %+v\n", polyDerivFirst)
	//fmt.Printf("polyDerivSecond: %+v\n", polyDerivSecond)

	polyDerivFirstRoots, err := polyDerivFirst.RealRoots()
	if err != nil {
		return 0.0, err
	}

	//fmt.Printf("polyDerivFirstRoots: %+v\n", polyDerivFirstRoots)

	if len(polyDerivFirstRoots) > 0 {
		minima := make([]float64, 0)

		for _, root := range polyDerivFirstRoots {
			evalSecondDeriv := polyDerivSecond.At(root)
			if evalSecondDeriv > 0 {
				//fmt.Printf("  found local minimum at: %f\n", root)
				minima = append(minima, root)
			}
			// This doesn't cover the case where evalSecondDeriv == 0, but this shouldn't
			// really happen with real work numbers. Either way, we don't care that much as
			// long as it doesn't occur very often.
		}

		if len(minima) > 0 {
			minimum := minima[0]
			for i := 1; i < len(minima); i++ {
				if poly.At(minima[i]) < poly.At(minimum) {
					minimum = minima[i]
				}
			}

			if poly.At(minimum) < poly.At(o.lowerBound) && poly.At(minimum) < poly.At(o.upperBound) {
				return minimum, nil
			}
		}
	}

	// No minimum found. So it's either at the left boundary or right boundary
	if poly.At(o.lowerBound) < poly.At(o.upperBound) {
		return o.lowerBound, nil
	} else {
		return o.upperBound, nil
	}
}

func (o *prOptimizer) InsertSample(powerLimit, pdp float64) int {
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

func (o *prOptimizer) CleanupOldSamples() {
	if len(o.samples) < o.maxSamples {
		return
	}

	// apply half life based cleanup
	indicesToRemove := make([]int, 0)
	testOrder := o.rand.Perm(len(o.samples))
	for _, i := range testOrder {
		if o.rand.Float64() > o.sampleHalfLifeProb {
			indicesToRemove = append(indicesToRemove, i)
		}

		if len(o.samples)-len(indicesToRemove) < o.maxSamples {
			break
		}
	}

	o.DeleteSamplesAtIndices(indicesToRemove)

	if len(o.samples) < o.maxSamples {
		return
	}

	// apply distance based cleanup
	distances := o.GetSampleDistances()

	//fmt.Printf("b distances: %+v\n", distances)
	SortSampleDistanceDescending(distances)
	//fmt.Printf("c distances: %+v\n", distances)
	distances = distances[o.maxSamples:]
	indicesToRemove = make([]int, len(distances))
	for i := 0; i < len(indicesToRemove); i++ {
		indicesToRemove[i] = distances[i].index
	}

	//fmt.Printf("d distances: %+v\n", distances)

	o.DeleteSamplesAtIndices(indicesToRemove)
}

func (o *prOptimizer) DeleteSamplesAtIndices(indicesToRemove []int) {
	sort.Ints(indicesToRemove)

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
}

func (o *prOptimizer) GetSampleDistances() []ds {
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

		distances[i].distance = dl + dr + 0.01*(o.rand.Float64()-0.5)*(o.upperBound-o.lowerBound)
		distances[i].index = i
	}
	return distances
}
