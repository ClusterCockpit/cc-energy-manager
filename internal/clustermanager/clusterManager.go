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
	"slices"

	"github.com/ClusterCockpit/cc-energy-manager/internal/jobmanager"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

type JobManagerId struct {
	DeviceType string
	JobId int64
}

type SubClusterId struct {
	Cluster           string
	SubCluster        string
}

type SubCluster struct {
	subClusterId      SubClusterId
	hostToJobMgrIds   map[string][]JobManagerId
	jobManagers       map[JobManagerId]*jobmanager.JobManager
	deviceTypeToOptimizerConfig map[string]json.RawMessage
}

type clusterManager struct {
	subClusters       map[SubClusterId]SubCluster
	done              chan bool
	wg                *sync.WaitGroup
	input             chan lp.CCMessage
	output            chan lp.CCMessage
	hostToSubcluster  map[string]SubClusterId
}

type ClusterManager interface {
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	Start()
	Close()
}

func (jmid JobManagerId) String() string {
	return fmt.Sprintf("%d-%s", jmid.JobId, jmid.DeviceType)
}

func (scid SubClusterId) String() string {
	return fmt.Sprintf("%s-%s", scid.Cluster, scid.SubCluster)
}

func (cm *clusterManager) AddCluster(rawClusterConfig json.RawMessage) error {
	clusterConfig := struct{
		Cluster        *string                    `json:"cluster"`
		SubCluster     *string                    `json:"subcluster"`
		DeviceTypes    map[string]json.RawMessage `json:"devicetypes"`
	}{}
	err := json.Unmarshal(rawClusterConfig, &clusterConfig)
	if err != nil {
		return fmt.Errorf("Unable to parse cluster JSON: %w", err)
	}

	if clusterConfig.Cluster == nil || clusterConfig.SubCluster == nil || clusterConfig.DeviceTypes == nil {
		return fmt.Errorf("cluster config is missing 'cluster', 'subcluster', or 'devicetypes': %s", string(rawClusterConfig))
	}

	subClusterId := SubClusterId{
		Cluster:    *clusterConfig.Cluster,
		SubCluster: *clusterConfig.SubCluster,
	}

	if _, ok := cm.subClusters[subClusterId]; ok {
		return fmt.Errorf("Cluster defined twice in config file: '%+v'", subClusterId)
	}

	cm.subClusters[subClusterId] = SubCluster{
		subClusterId:      subClusterId,
		hostToJobMgrIds:   make(map[string][]JobManagerId),
		jobManagers:       make(map[JobManagerId]*jobmanager.JobManager),
		deviceTypeToOptimizerConfig: clusterConfig.DeviceTypes,
	}
	return nil
}

func (cm *clusterManager) AddInput(input chan lp.CCMessage) {
	cm.input = input
}

func (cm *clusterManager) AddOutput(output chan lp.CCMessage) {
	cm.output = output
}

func (cm *clusterManager) StopJob(meta ccspecs.BaseJob) error {
	if len(meta.Cluster) == 0 || meta.JobID == 0 {
		return errors.New("job metadata does not contain data for cluster and jobid")
	}

	subClusterId := SubClusterId{
		Cluster: meta.Cluster,
		SubCluster: meta.SubCluster,
	}

	subCluster, ok := cm.subClusters[subClusterId]
	if !ok {
		return fmt.Errorf("Trying to stop a job on a cluster (%s), which we don't know", subClusterId)
	}

	// Iterate over all device types and stop each associated JobManager
	for deviceType, _ := range subCluster.deviceTypeToOptimizerConfig {
		jobManagerId := JobManagerId{ DeviceType: deviceType, JobId: meta.JobID }
		jobManager, ok := subCluster.jobManagers[jobManagerId]
		if ok {
			cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", jobManagerId), "Close jobmanager", jobManager)
			jobManager.Close()
			delete(subCluster.jobManagers, jobManagerId)
		} else {
			cclog.Errorf("Trying to stop ClusterManager(%s), which was never started", jobManagerId)
		}
	}

	// Delete JobManagerIds, that are assoicated to a host
	for _, r := range meta.Resources {
		jobMgrIds, ok := subCluster.hostToJobMgrIds[r.Hostname]
		if !ok {
			cclog.Errorf("Host '%s' is not associated with any jobs", r.Hostname)
			continue
		}

		// Delete all JobManagerIds, which are associated with the job's host, (for all device types)
		deleteFunc := func(jobManagerId JobManagerId) bool {
			return jobManagerId.JobId == meta.JobID
		}

		subCluster.hostToJobMgrIds[r.Hostname] = slices.DeleteFunc(jobMgrIds, deleteFunc)
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", subClusterId), fmt.Sprintf("Remove JobManager %d from JobManager lookup for %s", meta.JobID, r.Hostname))
	}

	return nil
}

func (cm *clusterManager) registerJob(subClusterId SubClusterId, jobManagerId JobManagerId, resources []*ccspecs.Resource) {
	subCluster, ok := cm.subClusters[subClusterId]
	if ok {
		cclog.Errorf("Cannot register job for subCluster %s, which is not available.")
		return
	}

	jobManagerConfig, ok := subCluster.deviceTypeToOptimizerConfig[jobManagerId.DeviceType]
	if !ok {
		cclog.Debugf("Not starting optimzier for deviceType %s, which has no optimizer configured", jobManagerId.DeviceType)
		return
	}

	jm, err := jobmanager.NewJobManager(cm.wg, subClusterId.Cluster, resources, jobManagerConfig)
	if err != nil {
		cclog.Errorf("Unable to create job manager: %v", err)
		return
	}

	jm.AddInput(jm.Input)

	subCluster.jobManagers[jobManagerId] = jm

	// Populate hostToJobMgrId Mapping
	for _, r := range resources {
		// Initially, the hostToJobMgrId list may not exist, so create it
		if _, ok := subCluster.hostToJobMgrIds[r.Hostname]; !ok {
			subCluster.hostToJobMgrIds[r.Hostname] = make([]JobManagerId, 0)
		}

		subCluster.hostToJobMgrIds[r.Hostname] = append(subCluster.hostToJobMgrIds[r.Hostname], jobManagerId)
	}

	jm.Start()
}

func (cm *clusterManager) StartJob(meta ccspecs.BaseJob) {
	if len(meta.Cluster) == 0 || meta.JobID == 0 {
		return
	}

	subClusterId := SubClusterId{
		Cluster: meta.Cluster,
		SubCluster: meta.SubCluster,
	}

	for _, r := range meta.Resources {
		cm.hostToSubcluster[r.Hostname] = subClusterId
	}

	// Start job for CPU Socket optimization
	jobManagerIdSocket := JobManagerId{
		DeviceType: "socket",
		JobId: meta.JobID,
	}
	cm.registerJob(subClusterId, jobManagerIdSocket, meta.Resources)

	// Start job for GPU optimization
	if meta.NumAcc > 0 {
		// Unconditionally start an optimizer for both Nvidia and AMD GPUs.
		// If no such optimizer is configured, we will catch that later.
		jobManagerIdNvGpu := JobManagerId{
			DeviceType: "nvidia_gpu",
			JobId: meta.JobID,
		}
		cm.registerJob(subClusterId, jobManagerIdNvGpu, meta.Resources)

		jobManagerIdAmdGpu := JobManagerId{
			DeviceType: "amd_gpu",
			JobId: meta.JobID,
		}
		cm.registerJob(subClusterId, jobManagerIdAmdGpu, meta.Resources)
	}
}

func unmarshalJob(payload string) (ccspecs.BaseJob, error) {
	var job ccspecs.BaseJob
	err := json.Unmarshal([]byte(payload), &job)
	if err != nil {
		cclog.ComponentError("ClusterManager", fmt.Sprintf("Failed to decode job payload: %s", payload))
		return job, err
	}
	return job, nil
}

func (cm *clusterManager) getSubcluster(hostname string) (SubClusterId, error) {
	if subClusterId, ok := cm.hostToSubcluster[hostname]; ok {
		return subClusterId, nil
	}
	return SubClusterId{}, fmt.Errorf("cannot find subcluster for host %s", hostname)
}

func (cm *clusterManager) processMetric(msg lp.CCMessage) {
	// Forward this metric to all JobManagers running on nodes,
	// that are currently associated with the message's hostname.
	hostname, ok := msg.GetTag("hostname")
	if !ok {
		cclog.ComponentError("ClusterManager", "Incoming message is missing tag 'hostname': %+v", msg)
		return
	}

	cluster, ok := msg.GetTag("cluster")
	if !ok {
		cclog.ComponentError("ClusterManager", "Incoming message is missing tag 'cluster': %+v", msg)
		return
	}

	subClusterId, err := cm.getSubcluster(hostname)
	if err != nil {
		cclog.ComponentError("ClusterManager", err.Error())
		return
	}

	if subClusterId.Cluster != cluster {
		// This case should usually not occur
		cclog.ComponentError("ClusterManager", "Received metric with inconsistent hostname <-> cluster mapping")
	}

	for _, jobMgrId := range cm.subClusters[subClusterId].hostToJobMgrIds[hostname] {
		if jobManager, ok := cm.subClusters[subClusterId].jobManagers[jobMgrId]; ok {
			jobManager.Input <- msg
		}
	}
}

func (cm *clusterManager) processEvent(msg lp.CCMessage) {
	if msg.Name() != "job" {
		cclog.Debugf("Ignoring incoming non-job event: %+v", msg)
		return
	}

	function, ok := msg.GetTag("function")
	if !ok {
		cclog.Error("Job event is missing tag 'function': %+v", msg)
		return
	}

	switch function {
	case "start_job":
		job, err := unmarshalJob(msg.GetEventValue())
		if err != nil {
			cclog.ComponentError("ClusterManager", err.Error())
			break
		}
		cm.StartJob(job)
	case "stop_job":
		job, err := unmarshalJob(msg.GetEventValue())
		if err != nil {
			cclog.ComponentError("ClusterManager", err.Error())
			break
		}
		err = cm.StopJob(job)
	default:
		cclog.Warn("Unimplemented job event: %+v", msg)
	}
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
					cm.processMetric(m)
				case lp.CCMSG_TYPE_EVENT:
					cm.processEvent(m)
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

	for _, c := range cm.subClusters {
		for jobManagerId, jobManager := range c.jobManagers {
			cclog.ComponentDebug("ClusterManager", "Send close to JobManager", jobManagerId)
			jobManager.Done <- true
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
	cm.subClusters = make(map[SubClusterId]SubCluster)
	cm.hostToSubcluster = make(map[string]SubClusterId)

	clusterConfigs := make([]json.RawMessage, 0)
	err := json.Unmarshal(config, &clusterConfigs)
	if err != nil {
		cclog.ComponentError("ccConfig", err.Error())
		return nil, err
	}

	for _, clusterConfig := range clusterConfigs {
		err = cm.AddCluster(clusterConfig)
		if err != nil {
			return nil, fmt.Errorf("AddCluster() failed: %w", err)
		}
	}

	return cm, nil
}
