// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

func TestInit(t *testing.T) {
	var wg sync.WaitGroup
	var job ccspecs.BaseJob
	testfile := "testjob.json"
	testconfig := `{
		"metrics" : [
			"instructions",
			"cpu_energy"
		],
		"interval" : "5s"
	}`
	jobFile, err := os.Open(testfile)
	if err != nil {
		t.Errorf("failed to open %s: %v", testfile, err.Error())
		return
	}
	defer jobFile.Close()
	jsonParser := json.NewDecoder(jobFile)
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Errorf("failed to decode %s: %v", testfile, err.Error())
		return
	}
	cclog.Init("debug", false)
	_, err = NewGssOptimizer("foobar", &wg, job, json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
}

func TestStart(t *testing.T) {
	var wg sync.WaitGroup
	var job ccspecs.BaseJob
	testfile := "testjob.json"
	testconfig := `{
		"metrics" : [
			"instructions",
			"cpu_energy"
		],
		"interval" : "5s"
	}`
	jobFile, err := os.Open(testfile)
	if err != nil {
		t.Errorf("failed to open %s: %v", testfile, err.Error())
		return
	}
	defer jobFile.Close()
	jsonParser := json.NewDecoder(jobFile)
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Errorf("failed to decode %s: %v", testfile, err.Error())
		return
	}
	cclog.Init("debug", false)
	o, err := NewGssOptimizer("foobar", &wg, job, json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
	o.Start()

	o.Close()
}

func TestStartInput(t *testing.T) {
	var wg sync.WaitGroup
	var job ccspecs.BaseJob
	testfile := "testjob.json"
	testconfig := `{
 		"metrics" : [
 			"instructions",
 			"cpu_energy"
 		],
 		"interval" : "5s"
 	}`
	testtags := map[string]string{
		"type":     "hwthread",
		"hostname": "a0805",
		"cluster":  "testcluster",
	}
	testmeta := map[string]string{
		"source": "testsource",
		"unit":   "Joules",
	}
	jobFile, err := os.Open(testfile)
	if err != nil {
		t.Errorf("failed to open %s: %v", testfile, err.Error())
		return
	}
	defer jobFile.Close()
	jsonParser := json.NewDecoder(jobFile)
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Errorf("failed to decode %s: %v", testfile, err.Error())
		return
	}
	cclog.Init("debug", false)
	o, err := NewGssOptimizer("foobar", &wg, job, json.RawMessage(testconfig))
	if err != nil {
		t.Errorf("failed to init GssOptimizer: %v", err.Error())
		return
	}
	input := make(chan lp.CCMessage)
	o.AddInput(input)
	o.Start()

	for i := 0; i < 21; i++ {
		for c := 0; c < 127; c++ {
			instr, _ := lp.NewMetric("instructions", testtags, testmeta, 1000.0*float64(i*c+1), time.Now())
			instr.AddTag("type-id", fmt.Sprintf("%d", c))
			input <- instr
		}
		for c := 0; c < 2; c++ {
			cpu_energy, _ := lp.NewMetric("cpu_energy", testtags, testmeta, 500*float64(i*c+1), time.Now())
			cpu_energy.AddTag("type", "socket")
			cpu_energy.AddTag("type-id", fmt.Sprintf("%d", c))
			input <- cpu_energy
		}
		time.Sleep(time.Second)
	}

	o.Close()
}

//
// func TestStartInputRegion(t *testing.T) {
// 	var wg sync.WaitGroup
// 	var job ccspecs.BaseJob
// 	testfile := "testjob.json"
// 	regionname := "foobar"
// 	testconfig := `{
// 		"metrics" : [
// 			"instructions",
// 			"cpu_energy"
// 		],
// 		"interval" : "5s"
// 	}`
// 	testtags := map[string]string{
// 		"type":     "hwthread",
// 		"hostname": "a0805",
// 		"cluster":  "testcluster",
// 	}
// 	testmeta := map[string]string{
// 		"source": "testsource",
// 		"unit":   "Joules",
// 	}
// 	jobFile, err := os.Open(testfile)
// 	if err != nil {
// 		t.Errorf("failed to open %s: %v", testfile, err.Error())
// 		return
// 	}
// 	defer jobFile.Close()
// 	jsonParser := json.NewDecoder(jobFile)
// 	err = jsonParser.Decode(&job)
// 	if err != nil {
// 		t.Errorf("failed to decode %s: %v", testfile, err.Error())
// 		return
// 	}
// 	cclog.Init("debug", false)
// 	o, err := NewGssOptimizer("foobar", &wg, job, json.RawMessage(testconfig))
// 	if err != nil {
// 		t.Errorf("failed to init GssOptimizer: %v", err.Error())
// 		return
// 	}
// 	input := make(chan lp.CCMessage)
// 	o.AddInput(input)
// 	o.Start()
// 	o.NewRegion(regionname)
//
// 	for i := 0; i < 21; i++ {
// 		for c := 0; c < 127; c++ {
// 			instr, _ := lp.NewMetric("instructions", testtags, testmeta, 1000.0*float64(i*c+1), time.Now())
// 			instr.AddTag("type-id", fmt.Sprintf("%d", c))
// 			input <- instr
// 		}
// 		for c := 0; c < 2; c++ {
// 			cpu_energy, _ := lp.NewMetric("cpu_energy", testtags, testmeta, 500*float64(i*c+1), time.Now())
// 			cpu_energy.AddTag("type", "socket")
// 			cpu_energy.AddTag("type-id", fmt.Sprintf("%d", c))
// 			input <- cpu_energy
// 		}
// 		time.Sleep(time.Second)
// 	}
// 	o.CloseRegion(regionname)
//
// 	o.Close()
// }
