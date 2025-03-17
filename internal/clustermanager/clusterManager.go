// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package clustermanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ClusterCockpit/cc-energy-manager/internal/jobmanager"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type subclusterEntry struct {
	name              string
	hostsToJobManager map[string][]string
	jobManagers       map[string]*jobmanager.JobManager
	config            json.RawMessage
}

type clusterManager struct {
	subclusters       map[string]subclusterEntry
	done              chan bool
	wg                *sync.WaitGroup
	input             chan lp.CCMessage
	output            chan lp.CCMessage
	hostsToSubcluster map[string]string
}

type ClusterManager interface {
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	Start()
	Close()
}

func (cm *clusterManager) AddCluster(key string, rawConfig json.RawMessage) {
	if _, ok := cm.subclusters[key]; !ok {
		ce := subclusterEntry{
			name:              key,
			hostsToJobManager: make(map[string][]string),
			jobManagers:       make(map[string]*jobmanager.JobManager),
			config:            rawConfig,
		}
		cm.subclusters[key] = ce
	}
}

func (cm *clusterManager) AddInput(input chan lp.CCMessage) {
	cm.input = input
}

func (cm *clusterManager) AddOutput(output chan lp.CCMessage) {
	cm.output = output
}

func (cm *clusterManager) CloseJob(meta ccspecs.BaseJob) error {
	if len(meta.Cluster) == 0 || meta.JobID == 0 {
		return errors.New("job metadata does not contain data for cluster and jobid")
	}

	sckey := fmt.Sprintf("%s-%s", meta.Cluster, meta.SubCluster)
	sc, ok := cm.subclusters[sckey]
	if !ok {
		return fmt.Errorf("unknown cluster %s, cannot shutdown optimizer for job %d", sckey, meta.JobID)
	}

	jmkey := fmt.Sprintf("%d", meta.JobID)
	if jm, ok := sc.jobManagers[jmkey]; ok {
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), "Close jobmanager", jm)
		jm.Close()
	}
	// Delete optimizer from the host->optimizer_list mapping
	for _, r := range meta.Resources {
		idx := -1
		if olist, ok := sc.hostsToJobManager[r.Hostname]; ok {
			// Find index in optimizer list
			for i, test := range olist {
				if test == jmkey {
					idx = i
					break
				}
			}
			if idx >= 0 {
				// Delete optimizer from list
				sc.hostsToJobManager[r.Hostname] = append(olist[:idx], olist[idx+1:]...)
				cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Remove optimizer %s from optimizer lookup for %s", jmkey, r.Hostname))
			} else {
				cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Cannot find optimizer %s for %s", jmkey, r.Hostname))
			}
		}
	}
	// Remove optimizer for job ID
	if _, ok := sc.jobManagers[jmkey]; ok {
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Remove optimizer for %s", jmkey))
		delete(sc.jobManagers, jmkey)
	}
	return nil
}

func (cm *clusterManager) registerJob(clusterName string, sckey string, jmkey string, resources []*ccspecs.Resource) {
	cluster, ok := cm.subclusters[sckey]
	if ok {
		return
	}

	// TODO is cm.subclusters[sckey] == clusterName?
	// if yes, then the parameter clusterName can be removed.
	// To me the code suggests cm.subclusters[sckey] is the subcluster name, which I don't want
	jm, _ := jobmanager.NewJobManager(cm.wg, clusterName, resources, cluster.config)
	jm.AddInput(jm.Input)

	cluster.jobManagers[jmkey] = jm

	for _, r := range resources {
		// host is unknown, so create new optimizer list
		if _, ok := cluster.hostsToJobManager[r.Hostname]; !ok {
			cluster.hostsToJobManager[r.Hostname] = make([]string, 0)
		}
		// Get list and add the job ID
		olist := cluster.hostsToJobManager[r.Hostname]
		olist = append(olist, jmkey)
		cluster.hostsToJobManager[r.Hostname] = olist
	}

	jm.Start()
}

func (cm *clusterManager) NewJob(meta ccspecs.BaseJob) {
	if len(meta.Cluster) == 0 || meta.JobID == 0 {
		return
	}

	for _, r := range meta.Resources {
		cm.hostsToSubcluster[r.Hostname] = meta.SubCluster
	}

	cm.registerJob(
		meta.Cluster,
		fmt.Sprintf("%s-%s", meta.Cluster, meta.SubCluster),
		fmt.Sprintf("%d", meta.JobID),
		meta.Resources)

	if meta.NumAcc > 0 {
		cm.registerJob(
			meta.Cluster,
			fmt.Sprintf("%s-%s-gpu", meta.Cluster, meta.SubCluster),
			fmt.Sprintf("%d", meta.JobID),
			meta.Resources)
	}
}

func (cm *clusterManager) isSupported(key string) bool {
	if _, ok := cm.subclusters[key]; !ok {
		return false
	}

	return true
}

func getJob(payload string) (ccspecs.BaseJob, error) {
	var job ccspecs.BaseJob
	err := json.Unmarshal([]byte(payload), &job)
	if err != nil {
		cclog.ComponentError("ClusterManager", fmt.Sprintf("Failed to decode job payload: %s", payload))
		return job, err
	}
	return job, nil
}

func (cm *clusterManager) getSubcluster(hostname string) (string, error) {
	if subcluster, ok := cm.hostsToSubcluster[hostname]; ok {
		return subcluster, nil
	}
	return "", fmt.Errorf("cannot find subcluster for host %s", hostname)
}

func (cm *clusterManager) Start() {
	cm.wg.Add(1)
	go func() {
		for {
			select {
			case <-cm.done:
				cm.wg.Done()
				close(cm.done)
				cclog.ComponentDebug("ClusterManager", "DONE")
				return
			case m := <-cm.input:
				mtype := m.MessageType()

				switch mtype {
				case lp.CCMSG_TYPE_METRIC:
					// forward all metrics to all job optimizers running on this node
					if h, ok := m.GetTag("hostname"); ok {
						if cluster, ok := m.GetTag("cluster"); ok {
							subcluster, err := cm.getSubcluster(h)
							if err != nil {
								cclog.ComponentError("ClusterManager", err.Error())
								continue
							}

							sckey := fmt.Sprintf("%s-%s", cluster, subcluster)
							if !cm.isSupported(sckey) {
								continue
							}
							for _, s := range cm.subclusters[sckey].hostsToJobManager[h] {
								if jm, ok := cm.subclusters[sckey].jobManagers[s]; ok {
									jm.Input <- m
								}
							}
						}
					}
				case lp.CCMSG_TYPE_EVENT:
					if m.Name() == "job" {
						if f, ok := m.GetTag("function"); ok {
							switch f {
							case "start_job":
								job, err := getJob(m.GetEventValue())
								if err != nil {
									cclog.ComponentError("ClusterManager", err.Error())
									break
								}
								cm.NewJob(job)
							case "stop_job":
								job, err := getJob(m.GetEventValue())
								if err != nil {
									cclog.ComponentError("ClusterManager", err.Error())
									break
								}
								err = cm.CloseJob(job)
							}
						}
					}
				}
			}
		}
	}()
	cclog.ComponentDebug("ClusterManager", "START")
}

func (cm *clusterManager) Close() {
	// Send close signal the cluster manager receive loop
	cm.done <- true
	// Iterate over optimizers to and close them

	for _, c := range cm.subclusters {
		for ident, s := range c.jobManagers {
			cclog.ComponentDebug("ClusterManager", "Send close to JobManager", ident)
			s.Done <- true
		}
	}
	cclog.ComponentDebug("ClusterManager", "All sessions closed")
	// Wait until the cluster manager receive loop finished
	<-cm.done
	cclog.ComponentDebug("ClusterManager", "CLOSE")
}

func NewClusterManager(wg *sync.WaitGroup, config json.RawMessage) (ClusterManager, error) {
	cm := new(clusterManager)

	cm.wg = wg
	cm.done = make(chan bool)
	cm.subclusters = make(map[string]subclusterEntry)
	cm.hostsToSubcluster = make(map[string]string)

	scConfigs := make(map[string]json.RawMessage)
	err := json.Unmarshal(config, &scConfigs)
	if err != nil {
		cclog.ComponentError("ccConfig", err.Error())
		return nil, err
	}

	for sc, scConfig := range scConfigs {
		if _, ok := cm.subclusters[sc]; !ok {
			cm.AddCluster(sc, scConfig)
		}
	}

	return cm, nil
}
