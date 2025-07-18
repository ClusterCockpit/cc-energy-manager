// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package clustermanager

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/ClusterCockpit/cc-energy-manager/internal/controller"
	"github.com/ClusterCockpit/cc-energy-manager/internal/jobmanager"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

//	type JobManagerId struct {
//		DeviceType string
//		JobId int64
//	}
type SubClusterId struct {
	Cluster    string
	SubCluster string
}

type Cluster struct {
	subClusters      map[string]*SubCluster
	jobIdToJob       map[int64]*Job
	hostToJobs       map[string]map[int64]*Job
	powerBudgetTotal float64
}

type SubCluster struct {
	subClusterId                SubClusterId
	deviceTypeToOptimizerConfig map[string]json.RawMessage
	hostRegex                   *regexp.Regexp
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
	ctrl               controller.Controller
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
	clusterConfig := struct {
		Cluster          *string                    `json:"cluster"`
		SubCluster       *string                    `json:"subcluster"`
		DeviceTypes      map[string]json.RawMessage `json:"devicetypes"`
		HostRegex        *string                    `json:"hostRegex"`
		PowerBudgetTotal float64                    `json:"powerBudgetTotal"`
	}{}

	err := json.Unmarshal(rawClusterConfig, &clusterConfig)
	if err != nil {
		return fmt.Errorf("unable to parse cluster JSON: %w", err)
	}

	if clusterConfig.Cluster == nil || clusterConfig.SubCluster == nil || clusterConfig.DeviceTypes == nil || clusterConfig.HostRegex == nil {
		return fmt.Errorf("cluster config is missing 'cluster', 'subcluster', 'devicetypes', or 'hostRegex': %s", string(rawClusterConfig))
	}

	if clusterConfig.PowerBudgetTotal <= 0.0 {
		return fmt.Errorf("cluster config must contain a non-zeor positive powerBudgetTotal")
	}

	subClusterId := SubClusterId{
		Cluster:    *clusterConfig.Cluster,
		SubCluster: *clusterConfig.SubCluster,
	}

	cclog.Debugf("Adding Cluster '%s'", subClusterId)

	cluster, ok := cm.clusters[*clusterConfig.Cluster]
	if !ok {
		cluster = &Cluster{
			subClusters:      make(map[string]*SubCluster),
			jobIdToJob:       make(map[int64]*Job),
			hostToJobs:       make(map[string]map[int64]*Job),
			powerBudgetTotal: clusterConfig.PowerBudgetTotal,
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
		cclog.Warnf("cannot stop job on cluster '%s', which we don't know", stopJobData.Cluster)
		return
	}

	job, ok := cluster.jobIdToJob[stopJobData.JobID]
	if !ok {
		cclog.Warnf("cannot stop job '%d', which we don't know", stopJobData.JobID)
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

func (cm *clusterManager) cleanupCollidingJobs(job *Job) {
	jobsToStop := make(map[int64]*ccspecs.BaseJob, 0)

	// Because jobs cannot cross multiple clusters, we only have to check
	// for overlapping resources on the same cluster.
	existingResources := make(map[string]*ccspecs.BaseJob)
	for _, runningJob := range cm.clusters[job.data.Cluster].jobIdToJob {
		for _, runningJobResource := range runningJob.data.Resources {
			// hwthreads
			for _, hwthread := range runningJobResource.HWThreads {
				resourceString := fmt.Sprintf("%s:hwt:%d", runningJobResource.Hostname, hwthread)
				existingResources[resourceString] = &runningJob.data
			}
			// accelerators
			for _, accelerator := range runningJobResource.Accelerators {
				resourceString := fmt.Sprintf("%s:acc:%s", runningJobResource.Hostname, accelerator)
				existingResources[resourceString] = &runningJob.data
			}
		}
	}

	for _, newJobResource := range job.data.Resources {
		for _, hwthread := range newJobResource.HWThreads {
			resourceString := fmt.Sprintf("%s:hwt:%d", newJobResource.Hostname, hwthread)
			if job, ok := existingResources[resourceString]; ok {
				jobsToStop[job.JobID] = job
			}
		}
		for _, accelerator := range newJobResource.Accelerators {
			resourceString := fmt.Sprintf("%s:acc:%s", newJobResource.Hostname, accelerator)
			if job, ok := existingResources[resourceString]; ok {
				jobsToStop[job.JobID] = job
			}
		}
	}

	if len(jobsToStop) > 0 {
		cclog.ComponentWarn("ClusterManager", "Cleaning up old jobs, which collide with new job:")

		for _, job := range jobsToStop {
			cclog.ComponentWarn("ClusterManager", "- [%s] %d", job.Cluster, job.JobID)
			cm.StopJob(ccspecs.BaseJob{JobID: job.JobID, Cluster: job.Cluster})
		}
	}
}

func (cm *clusterManager) registerJobManager(job *Job, deviceType string) {
	cluster := cm.clusters[job.data.Cluster]
	subCluster := cluster.subClusters[job.data.SubCluster]

	jobManagerConfig, ok := subCluster.deviceTypeToOptimizerConfig[deviceType]
	if !ok {
		cclog.Debugf("Not starting optimzier for deviceType '%s', which has no optimizer configured", deviceType)
		return
	}

	jm, err := jobmanager.NewJobManager(cm.ctrl, deviceType, job.data, jobManagerConfig)
	if err != nil {
		cclog.Errorf("Unable to create job manager: %v", err)
		return
	}

	job.deviceTypeToJobMgr[deviceType] = jm

	err = cm.UpdateJobPowerLimits(job.data.Cluster)
	if err != nil {
		cclog.Errorf("UpdateJobPowerLimits failed: %v", err)
	}

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
		cclog.Warnf("Cannot start job for unknown cluster '%s': %+v", startJobData.Cluster, startJobData)
		return
	}

	subCluster, ok := cluster.subClusters[startJobData.SubCluster]
	if !ok {
		cclog.Warnf("Cannot start job for unknown subcluster '%s': %+v", startJobData.SubCluster, startJobData)
		return
	}

	job := &Job{
		deviceTypeToJobMgr: make(map[string]*jobmanager.JobManager),
		data:               startJobData,
	}

	// If there already is a job running, which manages a resource, which
	// this new job is supposed to manage, stop that job.
	cm.cleanupCollidingJobs(job)

	// Register association between hostnames and Job IDs.
	for _, r := range job.data.Resources {
		jobs, ok := cluster.hostToJobs[r.Hostname]
		if !ok {
			jobs = make(map[int64]*Job)
			cluster.hostToJobs[r.Hostname] = jobs
		}
		jobs[job.data.JobID] = job
	}

	cluster.jobIdToJob[job.data.JobID] = job

	// Start job for CPU Socket optimization
	for deviceType := range subCluster.deviceTypeToOptimizerConfig {
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

	subClusterId, err := cm.GetSubClusterIdForHost(hostname)
	if err != nil {
		// Even on loglevel debug, this causes a lot of noise if a host is not managed by us.
		// Perhaps we want to add an explicit ignore list, which would make misconfiguration less likely.
		//cclog.ComponentError("ClusterManager", "Unable to determine cluster/subcluster for host '%s': %s", hostname, err)
		return
	}

	cluster, ok := cm.clusters[subClusterId.Cluster]
	if !ok {
		cclog.ComponentError("ClusterManager", "Unable to process metric for unknown cluster:", subClusterId.Cluster)
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
		cclog.Errorf("Job event is missing tag 'function': %+v", msg)
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
		cclog.Warnf("Unimplemented job event: %+v", msg)
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

func NewClusterManager(ctrl controller.Controller, config json.RawMessage) (ClusterManager, error) {
	cm := new(clusterManager)

	cm.done = make(chan struct{})
	cm.clusters = make(map[string]*Cluster)
	cm.hostToSubClusterId = make(map[string]SubClusterId)
	cm.ctrl = ctrl

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
		return fmt.Errorf("Cannot determine subcluster for job, which has no resources/hosts: %+v\n", job)
	}

	var subClusterId SubClusterId
	subClusterIdFound := false
	for _, r := range job.Resources {
		subClusterIdCandidate, err := cm.GetSubClusterIdForHost(r.Hostname)
		if err != nil {
			return err
		}
		if subClusterIdCandidate.Cluster != job.Cluster {
			return fmt.Errorf("configuration Error: Received job with hosts, which belong to a different cluster than configured: %+v", job)
		}
		if !subClusterIdFound {
			subClusterIdFound = true
			subClusterId = subClusterIdCandidate
		} else {
			return fmt.Errorf("job crosses multiple clusters: %+v", job)
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
						return SubClusterId{}, fmt.Errorf("hostname '%s' matches multiple regexes in the config", hostname)
					}

					subClusterId.Cluster = clusterName
					subClusterId.SubCluster = subClusterName
					cm.hostToSubClusterId[hostname] = subClusterId
					found = true
				}
			}
		}
		if !found {
			return SubClusterId{}, fmt.Errorf("hostname '%s' doesn't match any regex in the config", hostname)
		}
	}
	return subClusterId, nil
}

func (cm *clusterManager) UpdateJobPowerLimits(cluster string) error {
	if len(cm.clusters[cluster].jobIdToJob) == 0 {
		return nil
	}

	// We initially store only a 'weight' as the power value and we'll then later adjust
	// it to the actual power value:
	// map[jobId]map[deviceId]power
	jobIdToDeviceWeight := make(map[int64]map[string]float64)

	weightSum := 0.0
	for jobId, job := range cm.clusters[cluster].jobIdToJob {
		jobIdToDeviceWeight[jobId] = make(map[string]float64)

		for deviceId, jobManager := range job.deviceTypeToJobMgr {
			weight := jobManager.PowerBudgetWeight()
			weightSum += weight
			jobIdToDeviceWeight[jobId][deviceId] = weight
		}
	}

	if weightSum <= 0.0 {
		return fmt.Errorf("Unable to update power budget. Configured weights are negative!")
	}

	powerFactor := cm.clusters[cluster].powerBudgetTotal / weightSum

	for jobId, job := range cm.clusters[cluster].jobIdToJob {
		for deviceId, jobManager := range job.deviceTypeToJobMgr {
			jobManager.PowerBudgetSet(jobIdToDeviceWeight[jobId][deviceId] * powerFactor)
		}
	}

	return nil
}
