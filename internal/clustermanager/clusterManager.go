// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package clustermanager

import (
	"encoding/json"
	"fmt"
	"sync"
	"regexp"

	"github.com/ClusterCockpit/cc-energy-manager/internal/jobmanager"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

//type JobManagerId struct {
//	DeviceType string
//	JobId int64
//}
//
type SubClusterId struct {
	Cluster           string
	SubCluster        string
}

type Cluster struct {
	subClusters            map[string]*SubCluster
	jobIdToJob             map[int64]*Job
	hostToJobs             map[string]map[int64]*Job
}

type SubCluster struct {
	subClusterId      SubClusterId
	deviceTypeToOptimizerConfig map[string]json.RawMessage
	hostRegex         *regexp.Regexp
}

type Job struct {
	deviceTypeToJobMgr map[string]*jobmanager.JobManager
	data               ccspecs.BaseJob
}

type clusterManager struct {
	hostToSubClusterId map[string]SubClusterId
	clusters           map[string]*Cluster
	done               chan struct{}
	wg                 sync.WaitGroup
	input              chan lp.CCMessage
	output             chan lp.CCMessage
}

type ClusterManager interface {
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	Start()
	Close()
}

//func (jmid JobManagerId) String() string {
//	return fmt.Sprintf("%d/%s", jmid.JobId, jmid.DeviceType)
//}

func (scid SubClusterId) String() string {
	return fmt.Sprintf("%s/%s", scid.Cluster, scid.SubCluster)
}

func (cm *clusterManager) AddCluster(rawClusterConfig json.RawMessage) error {
	clusterConfig := struct{
		Cluster        *string                    `json:"cluster"`
		SubCluster     *string                    `json:"subcluster"`
		DeviceTypes    map[string]json.RawMessage `json:"devicetypes"`
		HostRegex      *string                    `json:"hostRegex"`
	}{}

	err := json.Unmarshal(rawClusterConfig, &clusterConfig)
	if err != nil {
		return fmt.Errorf("Unable to parse cluster JSON: %w", err)
	}

	if clusterConfig.Cluster == nil || clusterConfig.SubCluster == nil || clusterConfig.DeviceTypes == nil || clusterConfig.HostRegex == nil {
		return fmt.Errorf("cluster config is missing 'cluster', 'subcluster', 'devicetypes', or 'hostRegex': %s", string(rawClusterConfig))
	}

	subClusterId := SubClusterId{
		Cluster:    *clusterConfig.Cluster,
		SubCluster: *clusterConfig.SubCluster,
	}

	cclog.Debugf("Adding Cluster '%s'", subClusterId)

	cluster, ok := cm.clusters[*clusterConfig.Cluster]
	if !ok {
		cluster = &Cluster{
			subClusters:            make(map[string]*SubCluster),
			jobIdToJob:             make(map[int64]*Job),
			hostToJobs:             make(map[string]map[int64]*Job),
		}
		cm.clusters[*clusterConfig.Cluster] = cluster
	}

	if _, ok := cluster.subClusters[*clusterConfig.SubCluster]; ok {
		return fmt.Errorf("Cluster defined twice in config file: '%+v'", subClusterId)
	}

	cluster.subClusters[*clusterConfig.SubCluster] = &SubCluster{
		subClusterId:                subClusterId,
		deviceTypeToOptimizerConfig: clusterConfig.DeviceTypes,
		hostRegex:                   regexp.MustCompile(*clusterConfig.HostRegex),
	}
	return nil
}

func (cm *clusterManager) AddInput(input chan lp.CCMessage) {
	cm.input = input
}

func (cm *clusterManager) AddOutput(output chan lp.CCMessage) {
	cm.output = output
}

func (cm *clusterManager) StopJob(stopJobData ccspecs.BaseJob) {
	// WARNING: stopJobData is reconstructed from the StopJob event and is incomplete.
	// Be careful which values you use and get the actual job data from the JobManager
	if len(stopJobData.Cluster) == 0 || stopJobData.JobID == 0 {
		cclog.Warnf("job metadata does not contain data for cluster and jobid")
		return
	}

	cluster, ok := cm.clusters[stopJobData.Cluster]
	if !ok {
		cclog.Warnf("Cannot stop job on cluster '%s', which we don't know: %s", stopJobData.Cluster, stopJobData)
		return
	}

	job, ok := cluster.jobIdToJob[stopJobData.JobID]
	if !ok {
		cclog.Warnf("Cannot stop job '%d', which we don't know: %s", stopJobData.JobID, stopJobData)
		return
	}

	if len(stopJobData.SubCluster) > 0 && stopJobData.SubCluster != job.data.SubCluster {
		cclog.Errorf("Trying to stop jobId '%d' for subCluster '%s', which is different to subcluster started with '%s'", stopJobData.JobID, stopJobData.SubCluster, job.data.SubCluster)
		return
	}

	if stopJobData.Cluster != job.data.Cluster {
		// This should not occur if our code is correct, since cm.clusters[stopJobData.Cluster]
		// should not contain any jobs, that don't belong to it.
		cclog.Fatalf("Internal cluster consistency problem")
	}

	// Stop all JobManagers (multiple ones for each device type)
	for _, jobManager := range job.deviceTypeToJobMgr {
		jobManager.Close()
	}

	delete(cluster.jobIdToJob, job.data.JobID)

	// Delete assocation between a cluster's hostname and the job IDs running on that host.
	for _, r := range job.data.Resources {
		delete(cluster.hostToJobs[r.Hostname], job.data.JobID)
	}
}

func (cm *clusterManager) registerJobManager(job *Job, deviceType string) {
	cluster, _ := cm.clusters[job.data.Cluster]
	subCluster, _ := cluster.subClusters[job.data.SubCluster]

	jobManagerConfig, ok := subCluster.deviceTypeToOptimizerConfig[deviceType]
	if !ok {
		cclog.Debugf("Not starting optimzier for deviceType '%s', which has no optimizer configured", deviceType)
		return
	}

	jm, err := jobmanager.NewJobManager(deviceType, job.data, jobManagerConfig)
	if err != nil {
		cclog.Errorf("Unable to create job manager: %v", err)
		return
	}

	job.deviceTypeToJobMgr[deviceType] = jm

	// Which channel size should we choose here?
	// 100000 should be plenty for incoming CCMessages
	jm.AddInput(make(chan lp.CCMessage, 100000))
	jm.Start()
}

func (cm *clusterManager) StartJob(startJobData ccspecs.BaseJob) {
	if len(startJobData.Cluster) == 0 || startJobData.JobID == 0 {
		cclog.Warnf("Unable to start job, which is missing 'Cluster' or 'JobID'")
		return
	}

	// Reconstruct startJobData.SubCluster since we cannot rely it being sent to us.
	err := cm.JobPopulateSubcluster(&startJobData)
	if err != nil {
		cclog.Warnf("Cannot manage job energy for job: %v", err)
		return
	}

	cluster, ok := cm.clusters[startJobData.Cluster]
	if !ok {
		cclog.Warnf("Cannot start job for unknown cluster '%s': %s", startJobData.Cluster, startJobData)
		return
	}

	subCluster, ok := cluster.subClusters[startJobData.SubCluster]
	if !ok {
		cclog.Warnf("Cannot start job for unknown subcluster '%s': %s", startJobData.SubCluster, startJobData)
		return
	}

	job := &Job{
		deviceTypeToJobMgr: make(map[string]*jobmanager.JobManager),
		data: startJobData,
	}

	cluster.jobIdToJob[job.data.JobID] = job

	// Register association between hostnames and Job IDs.
	for _, r := range job.data.Resources {
		jobs, ok := cluster.hostToJobs[r.Hostname]
		if !ok {
			jobs = make(map[int64]*Job)
			cluster.hostToJobs[r.Hostname] = jobs
		}
		jobs[job.data.JobID] = job
	}

	// Start job for CPU Socket optimization
	for deviceType, _ := range subCluster.deviceTypeToOptimizerConfig {
		cm.registerJobManager(job, deviceType)
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

func (cm *clusterManager) processMetric(msg lp.CCMessage) {
	// Forward this metric to all JobManagers running on nodes,
	// that are currently associated with the message's hostname.
	if !msg.IsMetric() {
		cclog.ComponentError("ClusterManager", "Incoming message is not a metric: %+v", msg)
		return
	}

	hostname, ok := msg.GetTag("hostname")
	if !ok {
		cclog.ComponentError("ClusterManager", "Incoming message is missing tag 'hostname': %+v", msg)
		return
	}

	// The 'cluster' tag is not strictly necessary here, since we can also obtain it via GetSubClusterIdForHost
	// However, it allows us to do some sanity checking, which is probably a good thing.
	clusterName, ok := msg.GetTag("cluster")
	if !ok {
		cclog.ComponentError("ClusterManager", "Incoming message is missing tag 'cluster': %+v", msg)
		return
	}

	cluster, ok := cm.clusters[clusterName]
	if !ok {
		cclog.ComponentError("ClusterManager", "Unable to process metric for unknown cluster:", clusterName)
		return
	}

	subClusterId, err := cm.GetSubClusterIdForHost(hostname)
	if err != nil {
		cclog.ComponentError("ClusterManager", "Unable to determine cluster/subcluster for host '%s': %s", hostname, err)
		return
	}

	if subClusterId.Cluster != clusterName {
		// If everything is configured correctl, this case should usually not occur
		cclog.ComponentError("ClusterManager", "Received metric with inconsistent hostname <-> cluster mapping")
		return
	}

	jobs, ok := cluster.hostToJobs[hostname]
	if !ok {
		// This can occur in two cases:
		// - we are receiving a metric for a host we don't manage
		// - no job was started under that hostname yet, so the map does not exist yet
		return
	}

	// Finally, actually feed the metric data into the JobManager
	for _, job := range jobs {
		for _, jobManager := range job.deviceTypeToJobMgr {
			jobManager.Input <- msg
		}
	}
}

func (cm *clusterManager) processEvent(msg lp.CCMessage) {
	cclog.Debug("ClusterManager: processEvent()")
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
		cm.StopJob(job)
	default:
		cclog.Warn("Unimplemented job event: %+v", msg)
	}
}

func (cm *clusterManager) Start() {
	cm.wg.Add(1)

	cclog.ComponentDebug("JobManager", "Starting")

	go func() {
		for {
			select {
			case <-cm.done:
				cclog.ComponentDebug("ClusterManager", "Received Shutdown signal")
				for clusterName, cluster := range cm.clusters {
					for _, job := range cluster.jobIdToJob {
						for deviceType, jobManager := range job.deviceTypeToJobMgr {
							cclog.Debugf("Stopping JobManager cluster=%s jobId=%d deviceType=%s", clusterName, job.data.JobID, deviceType)
							jobManager.Close()
						}
					}
				}
				cm.wg.Done()
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
}

func (cm *clusterManager) Close() {
	cclog.ComponentDebug("ClusterManager", "Stopping ClusterManager...")
	cm.done <- struct{}{}
	cm.wg.Wait()
	cclog.ComponentDebug("ClusterManager", "Stopped ClusterManager!")
}

func NewClusterManager(config json.RawMessage) (ClusterManager, error) {
	cm := new(clusterManager)

	cm.done = make(chan struct{})
	cm.clusters = make(map[string]*Cluster)
	cm.hostToSubClusterId = make(map[string]SubClusterId)

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

func (cm *clusterManager) JobPopulateSubcluster(job *ccspecs.BaseJob) error {
	// Search the job's hostnames and find the respective subcluster
	if len(job.Resources) == 0 {
		// This case can only occur if someone is sending 'bad' start_job events without resources
		// or we are calling this function outside of a start_job context
		cclog.Fatalf("Cannot determine subcluster for job, which has no resources/hosts: %s\n", job)
	}

	var subClusterId SubClusterId
	subClusterIdFound := false
	for _, r := range job.Resources {
		subClusterIdCandidate, err := cm.GetSubClusterIdForHost(r.Hostname)
		if err != nil {
			return err
		}
		if subClusterIdCandidate.Cluster != job.Cluster {
			return fmt.Errorf("Configuration Error: Received job with hosts, which belong to a different cluster than configured: %s", job)
		}
		if !subClusterIdFound {
			subClusterIdFound = true
			subClusterId = subClusterIdCandidate
		} else {
			return fmt.Errorf("Job crosses multiple clusters: %s", job)
		}
	}

	if !subClusterIdFound {
		cclog.Fatalf("Determining SubCluster for a job failed. This case should not occur")
	}

	job.SubCluster = subClusterId.SubCluster
	return nil
}

func (cm *clusterManager) GetSubClusterIdForHost(hostname string) (SubClusterId, error) {
	subClusterId, ok := cm.hostToSubClusterId[hostname]
	if !ok {
		found := false
		for clusterName, cluster := range cm.clusters {
			for subClusterName, subcluster := range cluster.subClusters {
				if subcluster.hostRegex.MatchString(hostname) {
					if found {
						return SubClusterId{}, fmt.Errorf("Hostname '%s' matches multiple regexes in the config.", hostname)
					}

					subClusterId.Cluster = clusterName
					subClusterId.SubCluster = subClusterName
					cm.hostToSubClusterId[hostname] = subClusterId
					found = true
				}
			}
		}
		if !found {
			return SubClusterId{}, fmt.Errorf("Hostname '%s' doesn't match any regex in the config.", hostname)
		}
	}
	return subClusterId, nil
}
