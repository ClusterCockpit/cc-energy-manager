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

type jobSession struct {
	manager  jobmanager.Optimizer
	metadata ccspecs.BaseJob
	input    chan lp.CCMessage
	output   chan lp.CCMessage
	done     chan bool
}

type clusterEntry struct {
	name             string
	hosts2optimizers map[string][]string
	optimizers       map[string]jobSession
	maxBudget_watt   float64
	minBudget_watt   float64
	curBudget_watt   float64
}

type clusterManagerConfig struct {
	Budgets map[string]struct {
		MaxPowerBudget float64 `json:"max_power_budget"`
		MinPowerBudget float64 `json:"min_power_budget,omitempty"`
	} `json:"budgets"`
	Optimizer map[string]json.RawMessage `json:"optimizer"`
}

type clusterManager struct {
	clusters         map[string]clusterEntry
	done             chan bool
	wg               *sync.WaitGroup
	optWg            sync.WaitGroup
	input            chan lp.CCMessage
	output           chan lp.CCMessage
	config           clusterManagerConfig
	hosts2partitions map[string]string
}

type ClusterManager interface {
	Init(wg *sync.WaitGroup, config json.RawMessage) error
	AddCluster(cluster string)
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	CheckPowerBudget(cluster string, diff int) bool
	NewJob(meta ccspecs.BaseJob) error
	CloseJob(meta ccspecs.BaseJob) error
	Start()
	Close()
}

func (cm *clusterManager) Init(wg *sync.WaitGroup, config json.RawMessage) error {
	cm.wg = wg
	cm.done = make(chan bool)
	cm.clusters = make(map[string]clusterEntry)
	cm.hosts2partitions = make(map[string]string)
	err := json.Unmarshal(config, &cm.config)
	if err != nil {
		return err
	}
	// TODO initialize all configured clusters
	// If it is an unknown cluster, add it.
	// if _, ok := cm.clusters[c]; !ok {
	// 	cm.AddCluster(c)
	// 	// It is a non-configured cluster, go to next message
	// 	if _, ok := cm.clusters[c]; !ok {
	// 		cclog.ComponentError("ClusterManager", "Invalid cluster", c)
	// 		continue
	// 	}
	// }

	return nil
}

func (cm *clusterManager) CheckPowerBudget(cluster string, diff int) bool {
	if cluster, ok := cm.clusters[cluster]; ok {
		if cluster.maxBudget_watt == cluster.minBudget_watt && cluster.minBudget_watt == 0 {
			return true
		}
		if diff >= 0 && cluster.curBudget_watt+float64(diff) < cluster.maxBudget_watt {
			cluster.curBudget_watt += float64(diff)
			return true
		}
		if diff < 0 && cluster.curBudget_watt+float64(diff) > cluster.minBudget_watt {
			cluster.curBudget_watt += float64(diff)
			return true
		}
	}
	return false
}

func (cm *clusterManager) AddCluster(cluster string) {
	if _, ok := cm.clusters[cluster]; !ok {
		// If <cluster>-<partition> is not configured, ignore it
		if _, ok := cm.config.Budgets[cluster]; !ok {
			return
		}
		ce := clusterEntry{
			name:             cluster,
			hosts2optimizers: make(map[string][]string),
			optimizers:       make(map[string]jobSession),
			minBudget_watt:   0.0,
			maxBudget_watt:   0.0,
			curBudget_watt:   0.0,
		}
		if budget, ok := cm.config.Budgets[cluster]; ok {
			if budget.MinPowerBudget > 0 {
				ce.minBudget_watt = budget.MinPowerBudget
			}
			if budget.MaxPowerBudget > 0 {
				ce.maxBudget_watt = budget.MaxPowerBudget
			}
		}
		cclog.ComponentDebug("ClusterManager", fmt.Sprintf("Adding cluster %s with maximal power budget of %f W", cluster, ce.maxBudget_watt))
		cm.clusters[cluster] = ce
	}
}

func (cm *clusterManager) AddInput(input chan lp.CCMessage) {
	cm.input = input
}

func (cm *clusterManager) AddOutput(output chan lp.CCMessage) {
	cm.output = output
}

func (cm *clusterManager) CloseJob(meta ccspecs.BaseJob) error {
	if len(meta.Cluster) > 0 && meta.JobID > 0 {
		// mycluster := fmt.Sprintf("%s-%s", meta.Cluster, meta.Partition)
		mycluster := meta.Cluster
		// Get the optimizers for <cluster>-<partition>
		if cluster, ok := cm.clusters[mycluster]; ok {
			oid := fmt.Sprintf("%d", meta.JobID)
			// If an optimizer exists for the job ID, close it
			// if o, ok := cluster.optimizers[oid]; ok {
			// 	cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), "Close optimizer", oid)
			// 	// o.optimizer.Close()
			// }
			// Delete optimizer from the host->optimizer_list mapping
			for _, r := range meta.Resources {
				idx := -1
				if olist, ok := cluster.hosts2optimizers[r.Hostname]; ok {
					// Find index in optimizer list
					for i, test := range olist {
						if test == oid {
							idx = i
							break
						}
					}
					if idx >= 0 {
						// Delete optimizer from list
						cluster.hosts2optimizers[r.Hostname] = append(olist[:idx], olist[idx+1:]...)
						cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Remove optimizer %s from optimizer lookup for %s", oid, r.Hostname))
					} else {
						cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Cannot find optimizer %s for %s", oid, r.Hostname))
					}
				}
			}
			// Remove optimizer for job ID
			if _, ok := cluster.optimizers[oid]; ok {
				cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Remove optimizer for %s", oid))
				delete(cluster.optimizers, oid)
			}
			return nil
		} else {
			return fmt.Errorf("unknown cluster %s, cannot shutdown optimizer for job %d", mycluster, meta.JobID)
		}
	}
	return errors.New("job metadata does not contain data for cluster and jobid")
}

func (cm *clusterManager) NewJob(meta ccspecs.BaseJob) error {
	if len(meta.Cluster) > 0 && meta.JobID > 0 {
		// mycluster := fmt.Sprintf("%s-%s", meta.Cluster, meta.Partition)
		mycluster := meta.Cluster
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), "New job")
		// cluster := cm.clusters[mycluster]
		// Only accept jobs for configured <cluster>-<partition> entries
		//
		// if osettings, ok := cm.config.Optimizer[mycluster]; ok {
		// 	cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), "New optimizer for job", meta.JobID)
		// 	// Generate a new optimizer
		// 	o, err := optimizer.NewGssOptimizer(fmt.Sprintf("%s-%d", mycluster, meta.JobID), &cm.optWg, meta, osettings)
		// 	if err != nil {
		// 		err := fmt.Errorf("failed to start new GSS optimizer for Job %d", meta.JobID)
		// 		cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", mycluster), err.Error())
		// 		return err
		// 	}
		// 	// Create a new job session
		// 	j := jobSession{
		// 		optimizer: o,
		// 		metadata:  meta,
		// 		input:     make(chan lp.CCMessage),
		// 		output:    make(chan lp.CCMessage),
		// 		done:      make(chan bool),
		// 	}
		// 	// Add channels of job session to optimizer
		// 	o.AddInput(j.input)
		// 	o.AddOutput(j.output)
		// 	go func(job jobSession, allout chan lp.CCMessage) {
		// 		for {
		// 			select {
		// 			case <-job.done:
		// 				return
		// 			case msg := <-job.output:
		// 				allout <- msg
		// 			}
		// 		}
		// 	}(j, cm.output)
		// 	// Register optimizer using the job ID as key
		// 	cluster.optimizers[fmt.Sprintf("%d", meta.JobID)] = j
		// 	// When receiving messages, we get the cluster and host name but not the
		// 	// partion. Also the exact metric and at which level is unknown, thus the
		// 	// optimizer is registered at each host and a mapping host->partition is
		// 	// set up
		// 	for _, r := range meta.Resources {
		// 		// host is unknown, so create new optimizer list
		// 		if _, ok := cluster.hosts2optimizers[r.Hostname]; !ok {
		// 			cluster.hosts2optimizers[r.Hostname] = make([]string, 0)
		// 		}
		// 		// Get list and add the job ID
		// 		olist := cluster.hosts2optimizers[r.Hostname]
		// 		olist = append(olist, fmt.Sprintf("%d", meta.JobID))
		// 		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Adding optimizer lookup for %s -> %d", r.Hostname, meta.JobID))
		// 		cluster.hosts2optimizers[r.Hostname] = olist
		// 		// Set up host->partition mapping
		// 		if _, ok := cm.hosts2partitions[r.Hostname]; !ok {
		// 			cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Adding partition lookup for %s -> %s", r.Hostname, meta.Partition))
		// 			cm.hosts2partitions[r.Hostname] = meta.Partition
		// 		}
		// 	}
		// 	o.Start()
		// }

	}
	return nil
}

func (cm *clusterManager) isSupported(cluster string) bool {
	if _, ok := cm.clusters[cluster]; !ok {
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
				if cluster, ok := m.GetTag("cluster"); ok {
					if !cm.isSupported(cluster) {
						continue
					}
					switch mtype {
					case lp.CCMSG_TYPE_METRIC:
						// forward all metrics to all job optimizers running on this node
						if h, ok := m.GetTag("hostname"); ok {
							for _, s := range cm.clusters[cluster].hosts2optimizers[h] {
								if o, ok := cm.clusters[cluster].optimizers[s]; ok {
									o.input <- m
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
									err = cm.NewJob(job)
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
		}
	}()
	cclog.ComponentDebug("ClusterManager", "START")
}

func (cm *clusterManager) Close() {
	// Send close signal the cluster manager receive loop
	cm.done <- true
	// Iterate over optimizers to and close them

	for _, c := range cm.clusters {
		for ident, s := range c.optimizers {
			cclog.ComponentDebug("ClusterManager", "Send close to session", ident)
			// s.optimizer.Close()
			s.done <- true
			// close(s.input)
			// close(s.output)
		}
	}
	// Wait until all optimizers are closed
	cm.optWg.Wait()
	cclog.ComponentDebug("ClusterManager", "All sessions closed")
	// Wait until the cluster manager receive loop finished
	<-cm.done
	cclog.ComponentDebug("ClusterManager", "CLOSE")
}

func NewClusterManager(wg *sync.WaitGroup, config json.RawMessage) (ClusterManager, error) {
	cm := new(clusterManager)

	err := cm.Init(wg, config)
	if err != nil {
		return nil, err
	}

	return cm, err
}
