// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package clustermanager

import (
	"os"
	"testing"
	"encoding/json"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
)

func TestNew(t *testing.T) {
	cclog.Init("debug", false)
	configFile := "testconfig.json"
	configBytes, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatal(err.Error())
	}

	config := struct{
		Clusters json.RawMessage `json:"clusters"`
	}{}
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = NewClusterManager(config.Clusters)
	if err != nil {
		t.Fatalf("NewClusterManger() failed: %v", err)
	}
}

// func TestAddCluster(t *testing.T) {
// 	cclog.Init("debug", false)
// 	var wg sync.WaitGroup
// 	configFile := "testconfig.json"
// 	b, err := os.ReadFile(configFile)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	cm, err := NewClusterManager(&wg, b)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	cm.AddCluster("testcluster-partition")
// }
//
// func TestAddJob(t *testing.T) {
// 	cclog.Init("debug", false)
// 	var wg sync.WaitGroup
// 	configFile := "testconfig.json"
// 	b, err := os.ReadFile(configFile)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	jobFile := "testjob.json"
// 	cm, err := NewClusterManager(&wg, b)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	cm.AddCluster("testcluster-partition")
//
// 	jf, err := os.Open(jobFile)
// 	if err != nil {
// 		t.Errorf("failed to open %s: %v", jobFile, err.Error())
// 		return
// 	}
// 	defer jf.Close()
// 	jsonParser := json.NewDecoder(jf)
// 	var job ccspecs.BaseJob
// 	err = jsonParser.Decode(&job)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
//
// 	cm.NewJob(job)
// }
//
// func TestCloseJob(t *testing.T) {
// 	cclog.Init("debug", false)
// 	var wg sync.WaitGroup
// 	configFile := "testconfig.json"
// 	b, err := os.ReadFile(configFile)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	jobFile := "testjob.json"
// 	cm, err := NewClusterManager(&wg, b)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
// 	cm.AddCluster("testcluster-partition")
//
// 	jf, err := os.Open(jobFile)
// 	if err != nil {
// 		t.Errorf("failed to open %s: %v", jobFile, err.Error())
// 		return
// 	}
// 	defer jf.Close()
// 	jsonParser := json.NewDecoder(jf)
// 	var job ccspecs.BaseJob
// 	err = jsonParser.Decode(&job)
// 	if err != nil {
// 		t.Error(err.Error())
// 	}
//
// 	cm.NewJob(job)
//
// 	cm.CloseJob(job)
// }
