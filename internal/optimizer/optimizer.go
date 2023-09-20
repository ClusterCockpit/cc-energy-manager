// Copyright (C) 2023 NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"math"
)

var GOLDEN_RATIO float64 = (math.Sqrt(5) + 1) / 2

type Mode int

const (
	NarrowDown Mode = iota
	BroadenUp
	BroadenDown
)

type Limits struct {
	min, max, idle, step int
}

// GSS Struct
type GSS struct {
	x1, x2, x3, x4     int
	fx1, fx2, fx3, fx4 float64
	mode               Mode
	limits             Limits
}

func CreateGSS(x1, x2, x3, x4 int, fx1, fx2, fx3, fx4 float64, mode Mode, limits Limits) GSS {
	var golden GSS = GSS{x1, x2, x3, x4, fx1, fx2, fx3, fx4, mode, limits}
	return golden
}

// generates a new GSS object whose parameters are initialized to zero
func InitializeGSS(limit Limits) GSS {
	gss := CreateGSS(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, NarrowDown, limit)
	return gss
}

// dummy function the actual limits are found in the config for
// the optimizing parameter. For example, power limit, frequency etc
func InitializeLimits() Limits {
	limit := CreateLimits(0, 0, 0, 0)
	return limit
}

func CreateLimits(min, max, idle, step int) Limits {
	var lim Limits = Limits{min, max, idle, step}
	return lim
}

func Update(gss *GSS, x int, fx float64) {
	if gss.x1 == x {
		gss.fx1 = fx
	} else if gss.x2 == x {
		gss.fx2 = fx
	} else if gss.x3 == x {
		gss.fx3 = fx
	} else if gss.x4 == x {
		gss.fx4 = fx
	}
}

func UpdateGSS(gss *GSS, x int, y float64) int {

	Update(gss, x, y)

	if gss.mode == NarrowDown {
		return NarrowDownGSS(gss)
	} else if gss.mode == BroadenDown {
		return BroadenDownGSS(gss)
	} else {
		return BroadenUpGSS(gss)
	}
}

func SetLimits(gss GSS, min, max, idle, steps int) {
	gss.limits.idle = idle
	gss.limits.step = steps
	gss.limits.max = max
	gss.limits.min = min
}

func NarrowDownGSS(gss *GSS) int {
	// initializstion
	if gss.fx2 == 0 {
		return gss.x2
	}
	if gss.fx3 == 0 {
		return gss.x3
	}

	// Calculate ratio (after shifting borders)
	var b int = int(float64(gss.x4-gss.x2) / GOLDEN_RATIO)
	var new_c int = int((GOLDEN_RATIO - 1) * float64(gss.x3-gss.x2))
	// limits = gss._limits[gss.mode] set limits and pass down

	if gss.fx3 < gss.fx2 && new_c >= gss.limits.step {
		// Search higher
		gss.x1 = gss.x2
		gss.fx1 = gss.fx2
		gss.x2 = gss.x3
		gss.fx2 = gss.fx3
		gss.x3 = gss.x1 + b
		return gss.x3
	} else if gss.fx2 <= gss.fx3 && new_c >= gss.limits.step {
		// Search lower
		gss.x4 = gss.x3
		gss.fx4 = gss.fx3
		gss.x3 = gss.x2
		gss.fx3 = gss.fx2
		gss.x2 = gss.x4 - b
		return gss.x2
	} else {
		// Terminate narrow-down if step is too small
		gss.x4 = gss.x3 + new_c
		gss.fx4 = 0.0
		gss.x1 = gss.x2 - new_c
		gss.fx1 = 0.0
		if gss.mode == BroadenUp {
			gss.mode = BroadenDown
			return gss.x1
		} else {
			gss.mode = BroadenUp
			return gss.x4
		}
	}
}

func BroadenUpGSS(gss *GSS) int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64(gss.x3-gss.x2))
	var b int = int((GOLDEN_RATIO) * float64(gss.x4-gss.x3))
	//	limits =

	if gss.fx4 < gss.fx3 && float64(gss.x4)+(GOLDEN_RATIO+1)*float64(b) <= float64(gss.limits.max) {
		// Search higher
		gss.x3 = gss.x4
		gss.fx3 = gss.fx4
		gss.x4 = gss.x3 + a
		return gss.x4
	} else if gss.fx4 < gss.fx3 && b-(gss.x4-gss.x3) >= gss.limits.step {
		// Nearing limits -> reset exponential growth
		gss.x2 = gss.x3
		gss.fx2 = gss.fx3
		gss.x3 = gss.x4
		gss.fx3 = gss.fx4
		gss.x1 = gss.x3 - b
		gss.fx1 = 0.0
		gss.x4 = gss.x2 + b
		return gss.x4
	} else if gss.fx3 <= gss.fx4 && float64(gss.x4-gss.x3)/(GOLDEN_RATIO+1) >= float64(gss.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64(gss.x4-gss.x3))
		gss.x2 = gss.x3
		gss.fx2 = gss.fx3
		gss.x3 = gss.x4 - a
		SwitchToNarrowDown(gss)
		return gss.x3
	} else {
		// Moved past sweetspot or hitting step size
		if gss.x4-gss.x3 > gss.limits.step {
			// Move lower border up, if step size allows it
			// This speeds up the narrow-down
			gss.x2 = gss.x3
			gss.fx2 = gss.fx3
			gss.x3 = gss.x4
			gss.fx3 = gss.fx4
		}
		SwitchToNarrowDown(gss)
		return gss.x2
	}
}

func BroadenDownGSS(gss *GSS) int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64(gss.x3-gss.x2))
	var b int = int((GOLDEN_RATIO) * float64(gss.x2-gss.x1))
	//	limits = self._limits[gss.mode]
	if gss.fx1 < gss.fx2 && gss.x1-int(GOLDEN_RATIO+1)*b >= gss.limits.min {
		// Search lower
		gss.x2 = gss.x1
		gss.fx2 = gss.fx1
		gss.x1 = gss.x2 - a
		return gss.x1
	} else if gss.fx1 < gss.fx2 && b-(gss.x2-gss.x1) >= gss.limits.step {
		// Out-of-limits -> reset exponential growth
		gss.x3 = gss.x2
		gss.fx3 = gss.fx2
		gss.x2 = gss.x1
		gss.fx2 = gss.fx1
		gss.x4 = gss.x2 + b
		gss.fx4 = 0.0
		gss.x1 = gss.x3 - b
		return gss.x1
	} else if gss.fx2 <= gss.fx1 && float64(gss.x2-gss.x1)/(GOLDEN_RATIO+1) >= float64(gss.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64(gss.x2-gss.x1))
		gss.x3 = gss.x2
		gss.fx3 = gss.fx2
		gss.x2 = gss.x1 + a
		SwitchToNarrowDown(gss)
		return gss.x2
	} else {
		// Moved past sweetspot or hitting step size
		if gss.x2-gss.x1 > gss.limits.step {
			// Move upper border down, if step size allows it
			// This speeds up the narrow-down
			gss.x3 = gss.x2
			gss.fx3 = gss.fx2
			gss.x2 = gss.x1
			gss.fx2 = gss.fx1
		}
		SwitchToNarrowDown(gss)
		return gss.x3
	}
}

func SwitchToNarrowDown(gss *GSS) {
	gss.mode = NarrowDown
	var a int = int(float64(gss.x3-gss.x2) * GOLDEN_RATIO)
	gss.x1 = gss.x2 - a
	gss.fx1 = 0.0
	gss.x4 = gss.x3 + a
	gss.fx4 = 0.0
}
