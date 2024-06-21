package clustermanager

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	ccspecs "github.com/ClusterCockpit/cc-energy-manager/pkg/CCSpecs"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
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
	var job ccspecs.CCJob
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
	var job ccspecs.CCJob
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Error(err.Error())
	}

	cm.NewJob(job)

	cm.CloseJob(job)
}
