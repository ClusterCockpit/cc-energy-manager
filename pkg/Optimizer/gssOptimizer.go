package optimizer

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	ccspecs "github.com/ClusterCockpit/cc-backend/pkg/schema"
	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
)

// the mode variable that corresponds narrow & broader options
type Mode int

// enums used to choose the optimizing strategy
const (
	NarrowDown Mode = iota
	BroadenUp
	BroadenDown
)

type gssOptimizerConfig struct {
	optimizerConfig
	Interval   string `json:"interval"`
	MaxProcess int    `json:"max_process,omitempty"`
	Limits     struct {
		Min  int `json:"minimum"`
		Max  int `json:"maximum"`
		Step int `json:"step"`
		Idle int `json:"idle"`
	} `json:"limits"`
	Borders struct {
		Lower_outer int `json:"lower_outer"`
		Lower_inner int `json:"lower_inner"`
		Upper_outer int `json:"upper_outer"`
		Upper_inner int `json:"upper_inner"`
	} `json:"borders,omitempty"`
}

type gssOptimizerMetric struct {
	types    map[string]float64
	testtype string
}

type gssOptimizerHost struct {
	metrics      map[string]gssOptimizerMetric
	fudge_factor int
}
type gssOptimizerLimits struct {
	min, max, idle, step int
}

type gssOptimizerData struct {
	tuning_lower_outer_border int
	tuning_lower_inner_border int
	tuning_upper_inner_border int
	tuning_upper_outer_border int
	metric_lower_outer_border float64
	metric_lower_inner_border float64
	metric_upper_inner_border float64
	metric_upper_outer_border float64
	mode                      Mode
	limits                    gssOptimizerLimits
	edplast                   float64
	powercap                  int
	calls                     int64
}

func (d *gssOptimizerData) Update(powercap int, edp float64) int {
	switch {
	case d.tuning_lower_outer_border == d.powercap:
		d.metric_lower_outer_border = edp
	case d.tuning_lower_inner_border == d.powercap:
		d.metric_lower_inner_border = edp
	case d.tuning_upper_inner_border == d.powercap:
		d.metric_upper_inner_border = edp
	case d.tuning_upper_outer_border == d.powercap:
		d.metric_upper_outer_border = edp
	}

	ret := 0
	switch d.mode {
	case NarrowDown:
		ret = d.NarrowDown()
	case BroadenDown:
		ret = d.BroadenDown()
	default:
		ret = d.BroadenUp()
	}
	return ret
}

func (d *gssOptimizerData) SwitchToNarrowDown() {
	d.mode = NarrowDown
	var a int = int(float64((d.tuning_upper_inner_border)-(d.tuning_lower_inner_border)) * GOLDEN_RATIO)
	d.tuning_lower_outer_border = d.tuning_lower_inner_border - a
	d.metric_lower_outer_border = 0.0
	d.tuning_upper_outer_border = d.tuning_upper_inner_border + a
	d.metric_upper_outer_border = 0.0
}

func (d *gssOptimizerData) NarrowDown() int {

	if d.metric_lower_inner_border == 0 {
		return d.tuning_lower_inner_border
	}
	if d.metric_upper_inner_border == 0 {
		return d.tuning_upper_inner_border
	}
	var border int = int(float64((d.tuning_upper_outer_border)-(d.tuning_lower_inner_border)) / GOLDEN_RATIO)
	var new_c int = int((GOLDEN_RATIO - 1) * float64((d.tuning_upper_inner_border)-(d.tuning_lower_inner_border)))

	if d.metric_upper_inner_border < d.metric_lower_inner_border && new_c >= d.limits.step {
		// Search higher
		d.tuning_lower_outer_border = d.tuning_lower_inner_border
		d.metric_lower_outer_border = d.metric_lower_inner_border
		d.tuning_lower_inner_border = d.tuning_upper_inner_border
		d.metric_lower_inner_border = d.metric_upper_inner_border
		d.tuning_upper_inner_border = d.tuning_lower_outer_border + border
		return d.tuning_upper_inner_border
	} else if d.metric_lower_inner_border <= d.metric_upper_inner_border && new_c >= d.limits.step {
		// Search lower
		d.tuning_upper_outer_border = d.tuning_upper_inner_border
		d.metric_upper_outer_border = d.metric_upper_inner_border
		d.tuning_upper_inner_border = d.tuning_lower_inner_border
		d.metric_upper_inner_border = d.metric_lower_inner_border
		d.tuning_lower_inner_border = d.tuning_upper_outer_border - border
		return d.tuning_lower_inner_border
	} else {
		// Terminate narrow-down if step is too small
		d.tuning_upper_outer_border = d.tuning_upper_inner_border + new_c
		d.metric_upper_outer_border = 0.0
		d.tuning_lower_outer_border = d.tuning_lower_inner_border - new_c
		d.metric_lower_outer_border = 0.0
		if d.mode == BroadenUp {
			d.mode = BroadenDown
			return d.tuning_lower_outer_border
		} else {
			d.mode = BroadenUp
			return d.tuning_upper_outer_border
		}
	}

}
func (d *gssOptimizerData) BroadenDown() int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64(d.tuning_upper_inner_border-d.tuning_lower_inner_border))
	var b int = int((GOLDEN_RATIO) * float64(d.tuning_upper_outer_border-d.tuning_upper_inner_border))
	//	limits =

	if d.metric_upper_outer_border < d.metric_upper_inner_border && float64(d.tuning_upper_outer_border)+(GOLDEN_RATIO+1)*float64(b) <= float64(d.limits.max) {
		// Search higher
		d.tuning_upper_inner_border = d.tuning_upper_outer_border
		d.metric_upper_inner_border = d.metric_upper_outer_border
		d.tuning_upper_outer_border = d.tuning_upper_inner_border + a
		return d.tuning_upper_outer_border
	} else if d.metric_upper_outer_border < d.metric_upper_inner_border && b-((d.tuning_upper_outer_border)-(d.tuning_upper_inner_border)) >= d.limits.step {
		// Nearing limits -> reset exponential growth
		d.tuning_lower_inner_border = d.tuning_upper_inner_border
		d.metric_lower_inner_border = d.metric_upper_inner_border
		d.tuning_upper_inner_border = d.tuning_upper_outer_border
		d.metric_upper_inner_border = d.metric_upper_outer_border
		d.tuning_lower_outer_border = d.tuning_upper_inner_border - b
		d.metric_lower_outer_border = 0.0
		d.tuning_upper_outer_border = d.tuning_lower_inner_border + b
		return d.tuning_upper_outer_border
	} else if d.metric_upper_inner_border <= d.metric_upper_outer_border && float64((d.tuning_upper_outer_border)-(d.tuning_upper_inner_border))/(GOLDEN_RATIO+1) >= float64(d.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64((d.tuning_upper_outer_border)-(d.tuning_upper_inner_border)))
		d.tuning_lower_inner_border = d.tuning_upper_inner_border
		d.metric_lower_inner_border = d.metric_upper_inner_border
		d.tuning_upper_inner_border = d.tuning_upper_outer_border - a
		d.SwitchToNarrowDown()
		return d.tuning_upper_inner_border
	} else {
		// Moved past sweetspot or hitting step size
		if ((d.tuning_upper_outer_border) - (d.tuning_upper_inner_border)) > d.limits.step {
			// Move lower border up, if step size allows it
			// This speeds up the narrow-down
			d.tuning_lower_inner_border = d.tuning_upper_inner_border
			d.metric_lower_inner_border = d.metric_upper_inner_border
			d.tuning_upper_inner_border = d.tuning_upper_outer_border
			d.metric_upper_inner_border = d.metric_upper_outer_border
		}
		d.SwitchToNarrowDown()
		return d.tuning_lower_inner_border
	}
}
func (d *gssOptimizerData) BroadenUp() int {
	// Calculate ratio (after shifting borders)
	var a int = int((GOLDEN_RATIO - 1) * float64((d.tuning_upper_inner_border)-(d.tuning_lower_inner_border)))
	var b int = int((GOLDEN_RATIO) * float64((d.tuning_lower_inner_border)-(d.tuning_lower_outer_border)))
	//	limits = self._limits[d.mode]
	if d.metric_lower_outer_border < d.metric_lower_inner_border && d.tuning_lower_outer_border-int(GOLDEN_RATIO+1)*b >= d.limits.min {
		// Search lower
		d.tuning_lower_inner_border = d.tuning_lower_outer_border
		d.metric_lower_inner_border = d.metric_lower_outer_border
		d.tuning_lower_outer_border = d.tuning_lower_inner_border - a
		d.tuning_upper_inner_border = d.tuning_lower_inner_border
		d.metric_upper_inner_border = d.metric_lower_inner_border
		d.tuning_lower_inner_border = d.tuning_lower_outer_border
		d.metric_lower_inner_border = d.metric_lower_outer_border
		d.tuning_upper_outer_border = d.tuning_lower_inner_border + b
		d.metric_upper_outer_border = 0.0
		d.tuning_lower_outer_border = d.tuning_upper_inner_border - b
		return d.tuning_lower_outer_border
	} else if d.metric_lower_inner_border <= d.metric_lower_outer_border && float64((d.tuning_lower_inner_border)-(d.tuning_lower_outer_border))/(GOLDEN_RATIO+1) >= float64(d.limits.step) {
		// Moved past sweetspot -> narrow-down (optimized)
		a = int((GOLDEN_RATIO - 1) * float64((d.tuning_lower_inner_border)-(d.tuning_lower_outer_border)))
		d.tuning_upper_inner_border = d.tuning_lower_inner_border
		d.metric_upper_inner_border = d.metric_lower_inner_border
		d.tuning_lower_inner_border = d.tuning_lower_outer_border + a
		d.SwitchToNarrowDown()
		return d.tuning_lower_inner_border
	} else {
		// Moved past sweetspot or hitting step size
		if (d.tuning_lower_inner_border)-(d.tuning_lower_outer_border) > d.limits.step {
			// Move upper border down, if step size allows it
			// This speeds up the narrow-down
			d.tuning_upper_inner_border = d.tuning_lower_inner_border
			d.metric_upper_inner_border = d.metric_lower_inner_border
			d.tuning_lower_inner_border = d.tuning_lower_outer_border
			d.metric_lower_inner_border = d.metric_lower_outer_border
		}
		d.SwitchToNarrowDown()
		return d.tuning_upper_inner_border
	}
}

type gssOptimizer struct {
	optimizer
	//last     float64
	config   gssOptimizerConfig
	cache    map[string]gssOptimizerHost
	interval time.Duration
	//edplast  map[string]float64
	data       map[string]gssOptimizerData
	regionname string
	region     map[string]gssOptimizerData
}

type GssOptimizer interface {
	Init(ident string, wg *sync.WaitGroup, metadata ccspecs.BaseJob, config json.RawMessage) error
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	NewRegion(regionname string)
	CloseRegion(regionname string)
	Start()
	Close()
}

func isSocketMetric(metric string) bool {
	return (strings.Contains(metric, "power") || strings.Contains(metric, "energy") || metric == "mem_bw")
}
func isAcceleratorMetric(metric string) bool {
	return strings.HasPrefix(metric, "acc_")
}

func (o *gssOptimizer) Init(ident string, wg *sync.WaitGroup, metadata ccspecs.BaseJob, config json.RawMessage) error {
	o.ident = fmt.Sprintf("GssOptimizer(%s)", ident)
	o.wg = wg
	o.metadata = metadata
	o.done = make(chan bool)
	o.started = false
	o.data = make(map[string]gssOptimizerData)
	o.region = make(map[string]gssOptimizerData)
	o.regionname = ""

	o.config.MaxProcess = 10
	o.config.Limits.Min = 140
	o.config.Limits.Max = 220
	o.config.Limits.Idle = 140
	o.config.Limits.Step = 1
	o.config.Borders.Lower_inner = 170558
	o.config.Borders.Lower_outer = 140000
	o.config.Borders.Upper_inner = 189442
	o.config.Borders.Upper_outer = 220000

	err := json.Unmarshal(config, &o.config)
	if err != nil {
		err := fmt.Errorf("failed to parse config: %v", err.Error())
		cclog.ComponentError(o.ident, err.Error())
		return err
	}

	t, err := time.ParseDuration(o.config.Interval)
	if err != nil {
		err := fmt.Errorf("failed to parse interval %s: %v", o.config.Interval, err.Error())
		cclog.ComponentError(o.ident, err.Error())
		return err
	}
	o.interval = t

	o.cache = make(map[string]gssOptimizerHost)
	for _, r := range metadata.Resources {
		if _, ok := o.cache[r.Hostname]; !ok {
			cclog.ComponentDebug(o.ident, "registering host", r.Hostname, "to cache")
			o.cache[r.Hostname] = gssOptimizerHost{
				metrics: make(map[string]gssOptimizerMetric),
			}
		}
		if _, ok := o.data[r.Hostname]; !ok {
			k := gssOptimizerData{
				calls:                     0,
				edplast:                   float64(0.0),
				tuning_lower_outer_border: o.config.Borders.Lower_outer,
				tuning_lower_inner_border: o.config.Borders.Lower_inner,
				tuning_upper_outer_border: o.config.Borders.Upper_outer,
				tuning_upper_inner_border: o.config.Borders.Upper_inner,
				mode:                      NarrowDown,
				limits: gssOptimizerLimits{
					min:  o.config.Limits.Min,
					max:  o.config.Limits.Max,
					step: o.config.Limits.Step,
					idle: o.config.Limits.Idle,
				},
			}
			// TODO: Ask Host for real limits and stuff
			o.data[r.Hostname] = k
		}
		hdata := o.cache[r.Hostname]
		for _, m := range o.config.Metrics {
			if _, ok := hdata.metrics[m]; !ok {
				if isAcceleratorMetric(m) {
					hdata.metrics[m] = gssOptimizerMetric{
						testtype: "accelerator",
						types:    make(map[string]float64),
					}
					mdata := hdata.metrics[m]
					for _, h := range r.Accelerators {
						mdata.types[h] = math.NaN()
					}
				} else if isSocketMetric(m) {
					hdata.metrics[m] = gssOptimizerMetric{
						testtype: "socket",
						types:    make(map[string]float64),
					}
					mdata := hdata.metrics[m]
					for _, h := range r.HWThreads {
						// TODO: socket resolve
						x := fmt.Sprintf("%d", h/64)
						if _, ok := mdata.types[x]; !ok {
							mdata.types[x] = math.NaN()
						}
					}
				} else {
					hdata.metrics[m] = gssOptimizerMetric{
						testtype: "hwthread",
						types:    make(map[string]float64),
					}
					mdata := hdata.metrics[m]
					for _, h := range r.HWThreads {
						mdata.types[fmt.Sprintf("%d", h)] = math.NaN()
					}
				}
				s := fmt.Sprintf("registering metric %s (type %s) to cache host %s", m, hdata.metrics[m].testtype, r.Hostname)
				cclog.ComponentDebug(o.ident, s)
			}
		}
	}

	return nil
}

func (os *gssOptimizer) Close() {
	if os.started {
		os.ticker.Stop()
		os.done <- true

		<-os.done
	}
	cclog.ComponentDebug(os.ident, "CLOSE")
}

func (os *gssOptimizer) AddToCache(m lp.CCMessage) {
	if hostname, ok := m.GetTag("hostname"); ok {
		if hdata, ok := os.cache[hostname]; ok {
			if mdata, ok := hdata.metrics[m.Name()]; ok {
				if t, ok := m.GetTag("type"); ok && t == mdata.testtype {
					if tid, ok := m.GetTag("type-id"); ok {
						if _, ok := mdata.types[tid]; ok {
							if v, ok := m.GetField("value"); ok {
								//cclog.ComponentDebug("OptimizerSession", "add to cache", m.String())
								if f64, err := valueToFloat64(v); err == nil {
									mdata.types[tid] = f64
								}
							} //else {
							// 	cclog.ComponentError("OptimizerSession", "no value", m.String())
							// }
						} //else {
						// 	cclog.ComponentError("OptimizerSession", "unregistered TID", tid)
						// }
					} //else {
					// 	cclog.ComponentError("OptimizerSession", "no type-id", m.String())
					// }
				} //else {
				// 	cclog.ComponentError("OptimizerSession", "no type or not matching with test type", m.String())
				// }
			} //else {
			// 	cclog.ComponentError("OptimizerSession", "unregistered metric", m.Name())
			// }
		} //else {
		// 	cclog.ComponentError("OptimizerSession", "unregistered host", hostname)
		// }
	} // else {
	// 	cclog.ComponentError("OptimizerSession", "no hostname", m.String())
	// }
}

var GOLDEN_RATIO float64 = (math.Sqrt(5) + 1) / 2

func valueToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	}
	return math.NaN(), fmt.Errorf("cannot convert %v to float64", value)
}

func (os *gssOptimizer) CheckCache() bool {
	hcount := 0
hosts_loop:
	for _, hdata := range os.cache {
		allmetrics := false
		mcount := 0
		for _, mdata := range hdata.metrics {
			alltypes := false
			tcount := 0
			for _, tdata := range mdata.types {
				if !math.IsNaN(tdata) {
					tcount++
				} else {
					break hosts_loop
				}
			}
			if tcount == len(mdata.types) {
				alltypes = true
			}
			if alltypes {
				mcount++
			} else {
				break hosts_loop
			}
		}
		if mcount == len(hdata.metrics) {
			allmetrics = true
		}
		if allmetrics {
			hcount++
		} else {
			break hosts_loop
		}
	}
	return hcount == len(os.cache)
}

func (os *gssOptimizer) CalcMetric() (map[string]float64, error) {
	out := make(map[string]float64)
	for hostname, hdata := range os.cache {
		values := make(map[string]float64)

		for metric, mdata := range hdata.metrics {
			mvalue := 0.0
			for t, tdata := range mdata.types {
				v, err := valueToFloat64(tdata)
				if err == nil && !math.IsNaN(v) {
					mvalue += v
				} else {
					return nil, fmt.Errorf("cannot convert value %v for %s/%s/%s%s to float64", tdata, hostname, metric, mdata.testtype, t)
				}
			}
			values[metric] = mvalue
		}

		out[hostname] = math.NaN()
		if instr, ok := values["instructions"]; ok {
			if energy, ok := values["cpu_energy"]; ok {

				out[hostname] = (float64(hdata.fudge_factor) + energy) / instr
			}
		}
	}
	return out, nil
}

func (os *gssOptimizer) ResetCache() {
	for _, hdata := range os.cache {
		for _, mdata := range hdata.metrics {
			for t := range mdata.types {
				mdata.types[t] = math.NaN()
			}
		}
	}
}

func (os *gssOptimizer) Start() {
	os.wg.Add(1)
	// Ticker for running the optimizer
	os.ticker = *time.NewTicker(os.interval)
	os.started = true
	results := make(map[string][]float64)
	go func() {

		toCache := func(m lp.CCMessage) {
			// If it is a log message, it is likely caused by one of the energy
			// managers control messages sent to the host. The log messages
			// tells whether the control message was processed successfully or
			// not.
			// TODO
			if lp.IsLogMessage(m) {
				return
			}
			// Add message to cache
			os.AddToCache(m)
			// Check if all metrics have arrived to calculate a new value
			if os.CheckCache() {
				// Get the calculated metric per host
				hostresults, err := os.CalcMetric()
				if err != nil {
					cclog.ComponentError(os.ident, err.Error())
				} else {
					cclog.ComponentDebug(os.ident, hostresults)
					// Add the new metric values to the input list for the optimizer
					for h, r := range hostresults {
						if _, ok := results[h]; !ok {
							results[h] = make([]float64, 0)
						}
						results[h] = append(results[h], r)
					}
					// Clear cache to be ready for new messages
					os.ResetCache()
				}
			}
		}

		for {
			select {
			case <-os.done:
				// Signal to stop the optimizer and exit
				os.wg.Done()
				close(os.done)
				cclog.ComponentDebug(os.ident, "DONE")
				return
			case m := <-os.input:
				// Receive messages
				toCache(m)
				// If there are more messages in the input channel, we process
				// them directly
				for i := 0; i < len(os.input) && i < os.config.MaxProcess; i++ {
					toCache(<-os.input)
				}

			case <-os.ticker.C:
				// Run the optimizer at each ticker tick
				// Iterate over the host and their result lists
				for h, rlist := range results {
					// Sort the list to get the median
					sort.Float64s(rlist)
					median := rlist[int(len(rlist)/2)]
					// Delete the result list of the host
					results[h] = results[h][:0]
					// Run the calculator
					if os.regionname != "" {
						// Host executes a code region
						d := os.region[h]
						if os.region[h].edplast > 0 && d.calls >= 2 {
							cclog.ComponentDebug(os.ident, "Analyse cache with GSS for region", os.regionname)
							d.powercap = d.Update(d.powercap, median)
							d.edplast = median
						} else {
							cclog.ComponentDebug(os.ident, "Saving EDP for next round for region", os.regionname)
							d.edplast = median
						}
						d.calls++
						os.region[h] = d
					} else {
						// Host is not in a region
						d := os.data[h]
						if os.data[h].edplast > 0 && d.calls >= 2 {
							cclog.ComponentDebug(os.ident, "Analyse cache with GSS")
							d.powercap = d.Update(d.powercap, median)
							d.edplast = median
						} else {
							cclog.ComponentDebug(os.ident, "Saving EDP for next round")
							d.edplast = median
						}
						d.calls++
						os.data[h] = d
					}
				}
			}
		}
	}()
	cclog.ComponentDebug(os.ident, "START")
}

func NewGssOptimizer(ident string, wg *sync.WaitGroup, metadata ccspecs.BaseJob, config json.RawMessage) (GssOptimizer, error) {
	o := new(gssOptimizer)

	err := o.Init(ident, wg, metadata, config)
	if err != nil {
		cclog.ComponentError(o.ident, "failed to initialize GssOptimizer")
		return nil, err
	}

	return o, err
}

func (gss *gssOptimizer) NewRegion(regionname string) {
	if gss.regionname != "" {
		cclog.ComponentError(gss.ident, "Optimizer already working on region", gss.regionname, ", close first")
		return
	}
	cclog.ComponentDebug(gss.ident, "New region", regionname)
	gss.regionname = regionname
	gss.ResetCache()

	for h, hdata := range gss.data {
		cclog.ComponentDebug(gss.ident, "Init region data for host", h)
		gss.region[h] = gssOptimizerData{
			calls:                     0,
			tuning_lower_outer_border: hdata.tuning_lower_outer_border,
			tuning_lower_inner_border: hdata.tuning_lower_inner_border,
			tuning_upper_outer_border: hdata.tuning_upper_outer_border,
			tuning_upper_inner_border: hdata.tuning_upper_inner_border,
			metric_lower_outer_border: hdata.metric_lower_outer_border,
			metric_lower_inner_border: hdata.metric_lower_inner_border,
			metric_upper_outer_border: hdata.metric_upper_outer_border,
			metric_upper_inner_border: hdata.metric_upper_inner_border,
			mode:                      hdata.mode,
			edplast:                   hdata.edplast,
			powercap:                  hdata.powercap,
			limits: gssOptimizerLimits{
				min:  hdata.limits.min,
				max:  hdata.limits.max,
				idle: hdata.limits.idle,
				step: hdata.limits.step,
			},
		}
	}

}

func (gss *gssOptimizer) CloseRegion(regionname string) {
	if gss.regionname == "" {
		cclog.ComponentError(gss.ident, "Optimizer not working on region", regionname, ", cannot close")
		return
	}
	if gss.regionname != regionname {
		cclog.ComponentError(gss.ident, "Optimizer working on region", gss.regionname, ", cannot close", regionname)
		return
	}
	cclog.ComponentDebug(gss.ident, "Close region", regionname)
	gss.regionname = ""
	for h, hdata := range gss.region {
		cclog.ComponentDebug(gss.ident, "Copy data for host", h, "to general GSS")
		gss.data[h] = gssOptimizerData{
			tuning_lower_outer_border: hdata.tuning_lower_outer_border,
			tuning_lower_inner_border: hdata.tuning_lower_inner_border,
			tuning_upper_outer_border: hdata.tuning_upper_outer_border,
			tuning_upper_inner_border: hdata.tuning_upper_inner_border,
			metric_lower_outer_border: hdata.metric_lower_outer_border,
			metric_lower_inner_border: hdata.metric_lower_inner_border,
			metric_upper_outer_border: hdata.metric_upper_outer_border,
			metric_upper_inner_border: hdata.metric_upper_inner_border,
			mode:                      hdata.mode,
			edplast:                   hdata.edplast,
			powercap:                  hdata.powercap,
			limits: gssOptimizerLimits{
				min:  hdata.limits.min,
				max:  hdata.limits.max,
				idle: hdata.limits.idle,
				step: hdata.limits.step,
			},
		}
	}
}
