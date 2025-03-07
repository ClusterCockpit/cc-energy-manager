// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

type metric struct {
	types    map[string]float64
	testtype string
}

type host struct {
	metrics      map[string]metric
	fudge_factor int
}

type metricCache map[string]host

//
// func (o *jobManager) InitCache(metadata ccspecs.BaseJob, config optimizerConfig) {
// 	o.cache = make(map[string]host)
// 	for _, r := range metadata.Resources {
// 		if _, ok := o.cache[r.Hostname]; !ok {
// 			cclog.ComponentDebug(o.ident, "registering host", r.Hostname, "to cache")
// 			o.cache[r.Hostname] = host{
// 				metrics: make(map[string]metric),
// 			}
// 		}
// 		hdata := o.cache[r.Hostname]
// 		for _, m := range config.Metrics {
// 			if _, ok := hdata.metrics[m]; !ok {
// 				if isAcceleratorMetric(m) {
// 					hdata.metrics[m] = metric{
// 						testtype: "accelerator",
// 						types:    make(map[string]float64),
// 					}
// 					mdata := hdata.metrics[m]
// 					for _, h := range r.Accelerators {
// 						mdata.types[h] = math.NaN()
// 					}
// 				} else if isSocketMetric(m) {
// 					hdata.metrics[m] = metric{
// 						testtype: "socket",
// 						types:    make(map[string]float64),
// 					}
// 					mdata := hdata.metrics[m]
// 					for _, h := range r.HWThreads {
// 						// TODO: socket resolve
// 						x := fmt.Sprintf("%d", h/64)
// 						if _, ok := mdata.types[x]; !ok {
// 							mdata.types[x] = math.NaN()
// 						}
// 					}
// 				} else {
// 					hdata.metrics[m] = metric{
// 						testtype: "hwthread",
// 						types:    make(map[string]float64),
// 					}
// 					mdata := hdata.metrics[m]
// 					for _, h := range r.HWThreads {
// 						mdata.types[fmt.Sprintf("%d", h)] = math.NaN()
// 					}
// 				}
// 				s := fmt.Sprintf("registering metric %s (type %s) to cache host %s", m, hdata.metrics[m].testtype, r.Hostname)
// 				cclog.ComponentDebug(o.ident, s)
// 			}
// 		}
// 	}
// }
//
// func (os *optimizer) AddToCache(m lp.CCMessage) {
// 	if hostname, ok := m.GetTag("hostname"); ok {
// 		if hdata, ok := os.cache[hostname]; ok {
// 			if mdata, ok := hdata.metrics[m.Name()]; ok {
// 				if t, ok := m.GetTag("type"); ok && t == mdata.testtype {
// 					if tid, ok := m.GetTag("type-id"); ok {
// 						if _, ok := mdata.types[tid]; ok {
// 							if v, ok := m.GetField("value"); ok {
// 								if f64, err := valueToFloat64(v); err == nil {
// 									cclog.ComponentDebug(os.ident, "add to cache", m.String())
// 									mdata.types[tid] = f64
// 								}
// 							} else {
// 								cclog.ComponentError(os.ident, "no value", m.String())
// 							}
// 						}
// 					} else {
// 						cclog.ComponentError(os.ident, "no type-id", m.String())
// 					}
// 				} else {
// 					cclog.ComponentError(os.ident, "no type or not matching with test type", m.String())
// 				}
// 			}
// 		} else {
// 			cclog.ComponentError(os.ident, "unregistered host", hostname)
// 		}
// 	} else {
// 		cclog.ComponentError(os.ident, "no hostname", m.String())
// 	}
// }
//
// func valueToFloat64(value interface{}) (float64, error) {
// 	switch v := value.(type) {
// 	case float64:
// 		return v, nil
// 	case float32:
// 		return float64(v), nil
// 	case int64:
// 		return float64(v), nil
// 	case uint64:
// 		return float64(v), nil
// 	case int32:
// 		return float64(v), nil
// 	case uint32:
// 		return float64(v), nil
// 	case int16:
// 		return float64(v), nil
// 	case uint16:
// 		return float64(v), nil
// 	case int8:
// 		return float64(v), nil
// 	case uint8:
// 		return float64(v), nil
// 	}
// 	return math.NaN(), fmt.Errorf("cannot convert %v to float64", value)
// }
//
// func (os *optimizer) CheckCache() bool {
// 	hcount := 0
// hosts_loop:
// 	for _, hdata := range os.cache {
// 		allmetrics := false
// 		mcount := 0
// 		for _, mdata := range hdata.metrics {
// 			alltypes := false
// 			tcount := 0
// 			for _, tdata := range mdata.types {
// 				if !math.IsNaN(tdata) {
// 					tcount++
// 				} else {
// 					break hosts_loop
// 				}
// 			}
// 			if tcount == len(mdata.types) {
// 				alltypes = true
// 			}
// 			if alltypes {
// 				mcount++
// 			} else {
// 				break hosts_loop
// 			}
// 		}
// 		if mcount == len(hdata.metrics) {
// 			allmetrics = true
// 		}
// 		if allmetrics {
// 			hcount++
// 		} else {
// 			break hosts_loop
// 		}
// 	}
// 	return hcount == len(os.cache)
// }
//
// func (os *optimizer) CalcMetric() (map[string]float64, error) {
// 	out := make(map[string]float64)
// 	for hostname, hdata := range os.cache {
// 		values := make(map[string]float64)
//
// 		for metric, mdata := range hdata.metrics {
// 			mvalue := 0.0
// 			for t, tdata := range mdata.types {
// 				v, err := valueToFloat64(tdata)
// 				if err == nil && !math.IsNaN(v) {
// 					mvalue += v
// 				} else {
// 					return nil, fmt.Errorf("cannot convert value %v for %s/%s/%s%s to float64", tdata, hostname, metric, mdata.testtype, t)
// 				}
// 			}
// 			values[metric] = mvalue
// 		}
//
// 		out[hostname] = math.NaN()
// 		if instr, ok := values["instructions"]; ok {
// 			if energy, ok := values["cpu_energy"]; ok {
// 				out[hostname] = (float64(hdata.fudge_factor) + energy) / instr
// 			}
// 		}
// 	}
// 	return out, nil
// }
//
// func (os *optimizer) ResetCache() {
// 	for _, hdata := range os.cache {
// 		for _, mdata := range hdata.metrics {
// 			for t := range mdata.types {
// 				mdata.types[t] = math.NaN()
// 			}
// 		}
// 	}
// }
