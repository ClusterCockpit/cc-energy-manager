package main

import (
	"math"
//	"strings"
//	"encoding/json"
    "fmt"
//    "os"
//    "time"
//	"io/ioutil"
//	"github.com/nats-io/nats.go"
)

// the golden ratio variable the will be used in calculating the GSS
var GOLDEN_RATIO float64 = (math.Sqrt(5) + 1) / 2

// the mode variable that corresponds narrow & broader options
type Mode int

// enums used to choose the optimizing strategy
const (
	NarrowDown Mode = iota
	BroadenUp
	BroadenDown
)

// datastructure used in the GSS datastructure
type Limits struct {
	min, max, idle, step int
}

// The structure for data required from the node
type NodeInputStruct struct {
	nodeID 			string
	retiredInstructions 	int64
	coreEnergy		float64
}

// The structure for output data to be sent to the node 
type NodeOutputStruct struct {
	nodeID			string
	powerCap		float64
}

// GSS data structure
type GSS struct {
	tuning_lower_outer_border	int
	tuning_lower_inner_border	int
	tuning_upper_inner_border	int
	tuning_upper_outer_border	int
	metric_lower_outer_border	float64
	metric_lower_inner_border	float64
	metric_upper_inner_border	float64
	metric_upper_outer_border	float64
	mode				Mode
	limits				Limits
}


// GSS interface
type GSSI interface {
    InitGSS() 
    PrintGSSValues()
    UpdateBorders(powercap int, edp float64)

}


func (gss *GSS) PrintGSSValues() {
	fmt.Println("Printing out GSS values:")
	fmt.Println("tuning_lower_outer_border : ", gss.tuning_lower_outer_border)
	fmt.Println("tuning_lower_inner_border : ", gss.tuning_lower_inner_border)
	fmt.Println("tuning_upper_inner_border : ", gss.tuning_upper_inner_border)
        fmt.Println("tuning_upper_outer_border : ", gss.tuning_upper_outer_border)
        fmt.Println("metric_lower_outer_border : ", gss.metric_lower_outer_border)
        fmt.Println("metric_lower_inner_border : ", gss.metric_lower_inner_border)
        fmt.Println("metric_upper_inner_border : ", gss.metric_upper_inner_border)
        fmt.Println("metric_upper_outer_border : ", gss.metric_upper_outer_border)
        fmt.Println("tuning_lower_outer_border : ", gss.mode)
	fmt.Println("Limits")
        fmt.Println("Limits min : ", gss.limits.min)
	fmt.Println("Limits max : ", gss.limits.max)
        fmt.Println("Limits idle : ", gss.limits.idle)
        fmt.Println("Limits step : ", gss.limits.step)
}

// main function to test the GSS
func main() {

	// Initalize the GSS data structure
	fmt.Println("Initializing the GSS datastructure")
	g := new(GSS)
	g.InitGSS()

	// Print out GSS values
	g.PrintGSSValues()

	fmt.Println("Before calling the function SetFudgeFactor")
	// call the function SetFudgeFactor
	var fudge_factor int = SetFudgeFactor(110)
	fmt.Println("After calling the function SetFudgeFactor")

	var power_per_socket int = 184
	var retired_instr int = 69517960495
	var powercap int = 220000

        fmt.Println("Before calling the function CalculateEDP")
        // call the function CalculateEDP
        var edp float64 = CalculateEDP(fudge_factor, power_per_socket, retired_instr)
        fmt.Println("After calling the function CalculateEDP")

	// update boarders
	fmt.Println("update the GSS boarders")
	UpdateGSS(g, powercap, edp)

	fmt.Println("Print update GSS datastructure")
	g.PrintGSSValues()
}

// Initialize the GSS data structure
func (gss *GSS) InitGSS() {
        gss.tuning_lower_outer_border = 140000
        gss.tuning_lower_inner_border = 170558
        gss.tuning_upper_inner_border = 189442
        gss.tuning_upper_outer_border = 220000 
        gss.metric_lower_outer_border = 0
        gss.metric_lower_inner_border = 0
        gss.metric_lower_inner_border = 0
        gss.metric_upper_inner_border = 0
        gss.metric_upper_outer_border = 0
	gss.mode = NarrowDown
	gss.limits = CreateLimits(140, 220, 140, 1)
}


// calculate the energy delay product for the node if runtime isn't provided
// F here can represent “average node power without devices / num_devices”
// Or it can be set
func CalculateFudgeFactor(node_power_without_devices int, num_devices int) int {
	var fudge_factor int
	var tmp float64 = float64(node_power_without_devices) / float64(num_devices)
	fudge_factor = int(math.Floor(tmp))
	return fudge_factor
}

// if the fudge factor is predetermined we can set it
func SetFudgeFactor(value int) int {
	var fudge_factor int = value
	return  fudge_factor
}

// Caluculate EDP
// M = (F + power_per_socket)/retired_instructions
func CalculateEDP(fudge_factor int, power_per_socket int, retired_instructions int) float64 {
	fmt.Println("Function CalculateEDP")
	fmt.Println("fudge factor is ", fudge_factor," power per socket is ", power_per_socket, " retired instructions is ", retired_instructions)
	var edp float64 = (float64(fudge_factor) + float64(power_per_socket)) / float64(retired_instructions)
	fmt.Println("EDP value in CalculateEDP function is ", edp)
	return edp
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

func (gss *GSS) UpdateBorders(powercap int, edp float64) {
	fmt.Println("Entering UpdateBorders")
	fmt.Println("powercap is ", powercap, " and edp is ", edp)
	if gss.tuning_lower_outer_border == powercap {
		gss.metric_lower_outer_border = edp
		fmt.Println("tuning lower outer border == powercap")
		fmt.Println("metric lower outer boarder == %f", edp)
	} else if gss.tuning_lower_inner_border == powercap {
		gss.metric_lower_inner_border = edp
		fmt.Println("tuning lower inner border == powercap")
                fmt.Println("metric lower inner boarder == ", edp)
	} else if gss.tuning_upper_inner_border == powercap {
		gss.metric_upper_inner_border = edp
		fmt.Println("tuning upper inner border == powercap")
                fmt.Println("metric upper inner boarder == ", edp)
	} else if gss.tuning_upper_outer_border == powercap {
		gss.metric_upper_outer_border = edp
		fmt.Println("tuning upper outer border == powercap")
                fmt.Println("metric upper outer boarder == ", edp)
	} else {
	       fmt.Println("border not updated")
       }
}


// calculates powercap to be sent to the node
func UpdateGSS(gss *GSS, powercap int, edp float64) int {
	fmt.Printf("Entering Update GSS\n")
	fmt.Println("powercap is ", powercap ," and edp is ", edp, "\n")
	gss.UpdateBorders(powercap, edp)

	if gss.mode == NarrowDown {
		return NarrowDownGSS(gss)
	} else if gss.mode == BroadenDown {
		return BroadenDownGSS(gss)
	} else {
		return BroadenUpGSS(gss)
	}
}

// Calculate the GSS
// TODO: probably redundant need to check
func CalculateGSS(gss *GSS, powercap int, edp float64) {
	UpdateGSS(gss, powercap, edp)
}


func SetLimits(gss GSS, min, max, idle, steps int) {
	gss.limits.idle = idle
	gss.limits.step = steps
	gss.limits.max = max
	gss.limits.min = min
}

func NarrowDownGSS(gss *GSS) int {
	// initializstion
	if gss.metric_lower_inner_border == 0 {
		return gss.tuning_lower_inner_border
	}
	if gss.metric_upper_inner_border == 0 {
		return gss.tuning_upper_inner_border
	}

	// Calculate ratio (after shifting borders)
	var b int = int( float64( (gss.tuning_upper_outer_border) - (gss.tuning_lower_inner_border) ) / GOLDEN_RATIO)
	var new_c int = int((GOLDEN_RATIO - 1) * float64( (gss.tuning_upper_inner_border) -(gss.tuning_lower_inner_border) ))
	// limits = gss._limits[gss.mode] set limits and pass down

	if gss.metric_upper_inner_border < gss.metric_lower_inner_border && new_c >= gss.limits.step {
		// Search higher
		gss.tuning_lower_outer_border = gss.tuning_lower_inner_border
		gss.metric_lower_outer_border = gss.metric_lower_inner_border
		gss.tuning_lower_inner_border = gss.tuning_upper_inner_border
		gss.metric_lower_inner_border = gss.metric_upper_inner_border
		gss.tuning_upper_inner_border = gss.tuning_lower_outer_border + b
		return gss.tuning_upper_inner_border
	} else if gss.metric_lower_inner_border <= gss.metric_upper_inner_border && new_c >= gss.limits.step {
		// Search lower
		gss.tuning_upper_outer_border = gss.tuning_upper_inner_border
		gss.metric_upper_outer_border = gss.metric_upper_inner_border
		gss.tuning_upper_inner_border = gss.tuning_lower_inner_border
		gss.metric_upper_inner_border = gss.metric_lower_inner_border
		gss.tuning_lower_inner_border = gss.tuning_upper_outer_border - b
		return gss.tuning_lower_inner_border
	} else {
		// Terminate narrow-down if step is too small
		gss.tuning_upper_outer_border = gss.tuning_upper_inner_border + new_c
		gss.metric_upper_outer_border = 0.0
		gss.tuning_lower_outer_border = gss.tuning_lower_inner_border - new_c
		gss.metric_lower_outer_border = 0.0
		if gss.mode == BroadenUp {
			gss.mode = BroadenDown
			return gss.tuning_lower_outer_border
		} else {
			gss.mode = BroadenUp
			return gss.tuning_upper_outer_border
		}
	}
}

func BroadenUpGSS(gss *GSS) int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64(gss.tuning_upper_inner_border-gss.tuning_lower_inner_border))
	var b int = int((GOLDEN_RATIO) * float64(gss.tuning_upper_outer_border-gss.tuning_upper_inner_border))
	//	limits =

	if gss.metric_upper_outer_border < gss.metric_upper_inner_border && float64(gss.tuning_upper_outer_border)+(GOLDEN_RATIO+1)*float64(b) <= float64(gss.limits.max) {
		// Search higher
		gss.tuning_upper_inner_border = gss.tuning_upper_outer_border
		gss.metric_upper_inner_border = gss.metric_upper_outer_border
		gss.tuning_upper_outer_border = gss.tuning_upper_inner_border + a
		return gss.tuning_upper_outer_border
	} else if gss.metric_upper_outer_border < gss.metric_upper_inner_border && b-( (gss.tuning_upper_outer_border) - (gss.tuning_upper_inner_border) ) >= gss.limits.step {
		// Nearing limits -> reset exponential growth
		gss.tuning_lower_inner_border = gss.tuning_upper_inner_border
		gss.metric_lower_inner_border = gss.metric_upper_inner_border
		gss.tuning_upper_inner_border = gss.tuning_upper_outer_border
		gss.metric_upper_inner_border = gss.metric_upper_outer_border
		gss.tuning_lower_outer_border = gss.tuning_upper_inner_border - b
		gss.metric_lower_outer_border = 0.0
		gss.tuning_upper_outer_border = gss.tuning_lower_inner_border + b
		return gss.tuning_upper_outer_border
	} else if gss.metric_upper_inner_border <= gss.metric_upper_outer_border && float64( (gss.tuning_upper_outer_border) - (gss.tuning_upper_inner_border) )/(GOLDEN_RATIO+1) >= float64(gss.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64( (gss.tuning_upper_outer_border) - (gss.tuning_upper_inner_border) ))
		gss.tuning_lower_inner_border = gss.tuning_upper_inner_border
		gss.metric_lower_inner_border = gss.metric_upper_inner_border
		gss.tuning_upper_inner_border = gss.tuning_upper_outer_border - a
		SwitchToNarrowDown(gss)
		return gss.tuning_upper_inner_border
	} else {
		// Moved past sweetspot or hitting step size
		if ( (gss.tuning_upper_outer_border) - (gss.tuning_upper_inner_border) ) > gss.limits.step {
			// Move lower border up, if step size allows it
			// This speeds up the narrow-down
			gss.tuning_lower_inner_border = gss.tuning_upper_inner_border
			gss.metric_lower_inner_border = gss.metric_upper_inner_border
			gss.tuning_upper_inner_border = gss.tuning_upper_outer_border
			gss.metric_upper_inner_border = gss.metric_upper_outer_border
		}
		SwitchToNarrowDown(gss)
		return gss.tuning_lower_inner_border
	}
}

func BroadenDownGSS(gss *GSS) int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64( (gss.tuning_upper_inner_border) - (gss.tuning_lower_inner_border) ))
	var b int = int((GOLDEN_RATIO) * float64( (gss.tuning_lower_inner_border) - (gss.tuning_lower_outer_border) ))
	//	limits = self._limits[gss.mode]
	if gss.metric_lower_outer_border < gss.metric_lower_inner_border && gss.tuning_lower_outer_border-int(GOLDEN_RATIO+1)*b >= gss.limits.min {
		// Search lower
		gss.tuning_lower_inner_border = gss.tuning_lower_outer_border
		gss.metric_lower_inner_border = gss.metric_lower_outer_border
		gss.tuning_lower_outer_border = gss.tuning_lower_inner_border - a
		gss.tuning_upper_inner_border = gss.tuning_lower_inner_border
		gss.metric_upper_inner_border = gss.metric_lower_inner_border
		gss.tuning_lower_inner_border = gss.tuning_lower_outer_border
		gss.metric_lower_inner_border = gss.metric_lower_outer_border
		gss.tuning_upper_outer_border = gss.tuning_lower_inner_border + b
		gss.metric_upper_outer_border = 0.0
		gss.tuning_lower_outer_border = gss.tuning_upper_inner_border - b
		return gss.tuning_lower_outer_border
	} else if gss.metric_lower_inner_border <= gss.metric_lower_outer_border && float64( (gss.tuning_lower_inner_border) - (gss.tuning_lower_outer_border) )/(GOLDEN_RATIO+1) >= float64(gss.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64( (gss.tuning_lower_inner_border) - (gss.tuning_lower_outer_border) ))
		gss.tuning_upper_inner_border = gss.tuning_lower_inner_border
		gss.metric_upper_inner_border = gss.metric_lower_inner_border
		gss.tuning_lower_inner_border = gss.tuning_lower_outer_border + a
		SwitchToNarrowDown(gss)
		return gss.tuning_lower_inner_border
	} else {
		// Moved past sweetspot or hitting step size
		if (gss.tuning_lower_inner_border) - (gss.tuning_lower_outer_border) > gss.limits.step {
			// Move upper border down, if step size allows it
			// This speeds up the narrow-down
			gss.tuning_upper_inner_border = gss.tuning_lower_inner_border
			gss.metric_upper_inner_border = gss.metric_lower_inner_border
			gss.tuning_lower_inner_border = gss.tuning_lower_outer_border
			gss.metric_lower_inner_border = gss.metric_lower_outer_border
		}
		SwitchToNarrowDown(gss)
		return gss.tuning_upper_inner_border
	}
}

func SwitchToNarrowDown(gss *GSS) {
	gss.mode = NarrowDown
	var a int = int(float64( (gss.tuning_upper_inner_border) - (gss.tuning_lower_inner_border) ) * GOLDEN_RATIO)
	gss.tuning_lower_outer_border = gss.tuning_lower_inner_border - a
	gss.metric_lower_outer_border = 0.0
	gss.tuning_upper_outer_border = gss.tuning_upper_inner_border + a
	gss.metric_upper_outer_border = 0.0
}



// TODO: verify that LIKWID successfully modifies the settings
// TODO: if the state is optimized should we send data to change the hardware, will depend on frequency of data transfer
