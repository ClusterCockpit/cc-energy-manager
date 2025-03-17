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
	hostToJobIds      map[string][]int64
	jobManagers       map[int64]*jobmanager.JobManager
	config            json.RawMessage
}

type clusterManager struct {
	subclusters       map[string]subclusterEntry
	done              chan bool
	wg                *sync.WaitGroup
	input             chan lp.CCMessage
	output            chan lp.CCMessage
	hostToSubcluster  map[string]string
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
			hostToJobIds:      make(map[string][]int64),
			jobManagers:       make(map[int64]*jobmanager.JobManager),
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
		return fmt.Errorf("unknown cluster %s, cannot shutdown JobManager for job %d", sckey, meta.JobID)
	}

	if jm, ok := sc.jobManagers[meta.JobID]; ok {
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), "Close jobmanager", jm)
		jm.Close()
	}
	// Delete JobManager from the host->optimizer_list mapping
	for _, r := range meta.Resources {
		idx := -1
		if jobIds, ok := sc.hostToJobIds[r.Hostname]; ok {
			// Find index in optimizer list
			for i, test := range jobIds {
				if test == meta.JobID {
					idx = i
					break
				}
			}
			if idx >= 0 {
				// Delete JobManager from list
				sc.hostToJobIds[r.Hostname] = append(jobIds[:idx], jobIds[idx+1:]...)
				cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Remove JobManager %d from JobManager lookup for %s", meta.JobID, r.Hostname))
			} else {
				cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Cannot find JobManager %d for %s", meta.JobID, r.Hostname))
			}
		}
	}
	// Remove JobManager for job ID
	if _, ok := sc.jobManagers[meta.JobID]; ok {
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", sckey), fmt.Sprintf("Remove JobManager for %d", meta.JobID))
		delete(sc.jobManagers, meta.JobID)
	}
	return nil
}

func (cm *clusterManager) registerJob(clusterName string, sckey string, jobId int64, resources []*ccspecs.Resource) {
	cluster, ok := cm.subclusters[sckey]
	if ok {
		return
	}

	// TODO is cm.subclusters[sckey] == clusterName?
	// if yes, then the parameter clusterName can be removed.
	// To me the code suggests cm.subclusters[sckey] is the subcluster name, which I don't want
	jm, _ := jobmanager.NewJobManager(cm.wg, clusterName, resources, cluster.config)
	jm.AddInput(jm.Input)

	cluster.jobManagers[jobId] = jm

	for _, r := range resources {
		// host is unknown, so create new optimizer list
		if _, ok := cluster.hostToJobIds[r.Hostname]; !ok {
			cluster.hostToJobIds[r.Hostname] = make([]int64, 0)
		}
		// Get list and add the job ID
		cluster.hostToJobIds[r.Hostname] = append(cluster.hostToJobIds[r.Hostname], jobId)
	}

	jm.Start()
}

func (cm *clusterManager) NewJob(meta ccspecs.BaseJob) {
	if len(meta.Cluster) == 0 || meta.JobID == 0 {
		return
	}

	for _, r := range meta.Resources {
		cm.hostToSubcluster[r.Hostname] = meta.SubCluster
	}

	cm.registerJob(
		meta.Cluster,
		fmt.Sprintf("%s-%s", meta.Cluster, meta.SubCluster),
		meta.JobID,
		meta.Resources)

	if meta.NumAcc > 0 {
		cm.registerJob(
			meta.Cluster,
			fmt.Sprintf("%s-%s-gpu", meta.Cluster, meta.SubCluster),
			meta.JobID,
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
	if subcluster, ok := cm.hostToSubcluster[hostname]; ok {
		return subcluster, nil
	}
	return "", fmt.Errorf("cannot find subcluster for host %s", hostname)
}

func checkRequiredTags(msg lp.CCMessage, requiredTags []string) bool {
	// TODO maybe this extra function isn't necessary after all...
	for _, requiredTag := range requiredTags {
		if _, ok := msg.GetTag(requiredTag); !ok {
			cclog.Error("Incoming message is missing tag '%s': %+v", requiredTag, msg)
			return false
		}
	}
	return true
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
					if !checkRequiredTags(m, []string{"hostname", "cluster"}) {
						break
					}

					hostname, _ := m.GetTag("hostname")
					cluster, _ := m.GetTag("cluster")
					subcluster, err := cm.getSubcluster(hostname)
					if err != nil {
						cclog.ComponentError("ClusterManager", err.Error())
						continue
					}

					// TODO this %s-%s key thing won't work properly, this needs rework
					// We need support for a cluster-subcluster combination, which is identifiable
					// regardless whether it uses a GPU or not.
					sckey := fmt.Sprintf("%s-%s", cluster, subcluster)
					if !cm.isSupported(sckey) {
						continue
					}

					for _, s := range cm.subclusters[sckey].hostToJobIds[hostname] {
						if jm, ok := cm.subclusters[sckey].jobManagers[s]; ok {
							jm.Input <- m
						}
					}
				case lp.CCMSG_TYPE_EVENT:
					if m.Name() != "job" {
						break
					}
					function, ok := m.GetTag("function")
					if !ok {
						cclog.Error("Job event is missing tag 'function': %+v", m)
						break
					}

					switch function {
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
	cm.hostToSubcluster = make(map[string]string)

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
