// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package clustermanager

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

func TestNew(t *testing.T) {
	var wg sync.WaitGroup
	configFile := "testconfig.json"
	_, err := NewClusterManager(&wg, configFile)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestAddCluster(t *testing.T) {
	var wg sync.WaitGroup
	configFile := "testconfig.json"
	cm, err := NewClusterManager(&wg, configFile)
	if err != nil {
		t.Error(err.Error())
	}
	cm.AddCluster("testcluster-partition")
}

func TestAddJob(t *testing.T) {
	var wg sync.WaitGroup
	configFile := "testconfig.json"
	jobFile := "testjob.json"
	cclog.SetDebug()
	cm, err := NewClusterManager(&wg, configFile)
	if err != nil {
		t.Error(err.Error())
	}
	cm.AddCluster("testcluster-partition")

	jf, err := os.Open(jobFile)
	if err != nil {
		t.Errorf("failed to open %s: %v", jobFile, err.Error())
		return
	}
	defer jf.Close()
	jsonParser := json.NewDecoder(jf)
	var job ccspecs.BaseJob
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Error(err.Error())
	}

	cm.NewJob(job)
}

func TestCloseJob(t *testing.T) {
	var wg sync.WaitGroup
	configFile := "testconfig.json"
	jobFile := "testjob.json"
	cclog.SetDebug()
	cm, err := NewClusterManager(&wg, configFile)
	if err != nil {
		t.Error(err.Error())
	}
	cm.AddCluster("testcluster-partition")

	jf, err := os.Open(jobFile)
	if err != nil {
		t.Errorf("failed to open %s: %v", jobFile, err.Error())
		return
	}
	defer jf.Close()
	jsonParser := json.NewDecoder(jf)
	var job ccspecs.BaseJob
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Error(err.Error())
	}

	cm.NewJob(job)

	cm.CloseJob(job)
}
