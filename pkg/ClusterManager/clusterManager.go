package clustermanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	ccspecs "github.com/ClusterCockpit/cc-backend/pkg/schema"
	optimizer "github.com/ClusterCockpit/cc-energy-manager/pkg/Optimizer"
	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
	cclog "github.com/ClusterCockpit/cc-metric-collector/pkg/ccLogger"
)

type jobSession struct {
	optimizer optimizer.Optimizer
	metadata  ccspecs.BaseJob
	input     chan lp.CCMessage
	output    chan lp.CCMessage
}

type clusterEntry struct {
	name             string
	hosts2optimizers map[string][]string
	optimizers       map[string]jobSession //optimizer.Optimizer
	maxBudget_watt   float64
	minBudget_watt   float64
	curBudget_watt   float64
}

type clusterManagerConfig struct {
	Budgets map[string]struct {
		MaxPowerBudget float64 `json:"max_power_budget"`
		MinPowerBudget float64 `json:"min_power_budget,omitempty"`
	} `json:"budgets"`
	Optimizer    map[string]json.RawMessage `json:"optimizer"`
	JobEventName string                     `json:"job_event_name"`
}

type clusterManager struct {
	clusters         map[string]clusterEntry
	done             chan bool
	wg               *sync.WaitGroup
	optWg            sync.WaitGroup
	input            chan lp.CCMessage
	output           chan lp.CCMessage
	configFile       string
	config           clusterManagerConfig
	hosts2partitions map[string]string
}

type ClusterManager interface {
	Init(wg *sync.WaitGroup, configFile string) error
	AddCluster(cluster string)
	AddInput(input chan lp.CCMessage)
	AddOutput(output chan lp.CCMessage)
	CheckPowerBudget(cluster string, diff int) bool
	NewJob(meta ccspecs.BaseJob) error
	CloseJob(meta ccspecs.BaseJob) error
	Start()
	Close()
}

func (cm *clusterManager) Init(wg *sync.WaitGroup, configFile string) error {
	cm.wg = wg
	cm.done = make(chan bool)
	cm.clusters = make(map[string]clusterEntry)
	cm.configFile = configFile
	cm.hosts2partitions = make(map[string]string)
	f, err := os.ReadFile(cm.configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(f, &cm.config)
	if err != nil {
		return err
	}

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
		mycluster := fmt.Sprintf("%s-%s", meta.Cluster, meta.Partition)
		if cluster, ok := cm.clusters[mycluster]; ok {
			oid := fmt.Sprintf("%d", meta.JobID)
			if o, ok := cluster.optimizers[oid]; ok {
				o.optimizer.Close()
			}
			for _, r := range meta.Resources {
				idx := -1
				if olist, ok := cluster.hosts2optimizers[r.Hostname]; ok {
					for i, test := range olist {
						if test == oid {
							idx = i
							break
						}
					}
					if idx >= 0 {
						cluster.hosts2optimizers[r.Hostname] = append(olist[:idx], olist[idx+1:]...)
						cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Remove optimizer %s from optimizer lookup for %s", oid, r.Hostname))
					} else {
						cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Cannot find optimizer %s for %s", oid, r.Hostname))
					}
				}
			}
			cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Remove optimizer for %d", meta.JobID))
			delete(cluster.optimizers, fmt.Sprintf("%d", meta.JobID))
			return nil
		} else {
			return fmt.Errorf("unknown cluster %s, cannot shutdown optimizer for job %d", meta.Cluster, meta.JobID)
		}
	}
	return errors.New("job metadata does not contain data for cluster and jobid")
}

func (cm *clusterManager) NewJob(meta ccspecs.BaseJob) error {
	if len(meta.Cluster) > 0 && meta.JobID > 0 {
		mycluster := fmt.Sprintf("%s-%s", meta.Cluster, meta.Partition)
		cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), "New job")

		cluster := cm.clusters[mycluster]
		if osettings, ok := cm.config.Optimizer[mycluster]; ok {
			cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), "New optimizer for job", meta.JobID)
			o, err := optimizer.NewGssOptimizer(fmt.Sprintf("%s-%d", mycluster, meta.JobID), &cm.optWg, meta, osettings)
			if err != nil {
				err := fmt.Errorf("failed to start new GSS optimizer for Job %d", meta.JobID)
				cclog.ComponentError(fmt.Sprintf("ClusterManager(%s)", mycluster), err.Error())
				return err
			}
			j := jobSession{
				optimizer: o,
				metadata:  meta,
				input:     make(chan lp.CCMessage),
				output:    make(chan lp.CCMessage),
			}
			o.AddInput(j.input)
			o.AddOutput(j.output)
			cluster.optimizers[fmt.Sprintf("%d", meta.JobID)] = j
			for _, r := range meta.Resources {
				if _, ok := cluster.hosts2optimizers[r.Hostname]; !ok {
					cluster.hosts2optimizers[r.Hostname] = make([]string, 0)
				}
				olist := cluster.hosts2optimizers[r.Hostname]
				olist = append(olist, fmt.Sprintf("%d", meta.JobID))
				cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Adding optimizer lookup for %s -> %d", r.Hostname, meta.JobID))
				cluster.hosts2optimizers[r.Hostname] = olist
				cclog.ComponentDebug(fmt.Sprintf("ClusterManager(%s)", mycluster), fmt.Sprintf("Adding partition lookup for %s -> %s", r.Hostname, meta.Partition))
				cm.hosts2partitions[r.Hostname] = meta.Partition
			}
			o.Start()
		}

	}
	return nil
}

func (cm *clusterManager) Start() {

	cm.wg.Add(1)
	go func() {
		for {
			select {
			case <-cm.done:
				cm.wg.Done()
				cclog.ComponentDebug("ClusterManager", "DONE")
				return
			case m := <-cm.input:
				mtype := m.MessageType()
				if c, ok := m.GetTag("cluster"); ok {
					if _, ok := cm.clusters[c]; !ok {
						cm.AddCluster(c)
					}
					if mtype == lp.CCMSG_TYPE_METRIC {
					if h, ok := m.GetTag("hostname"); ok {
						if p, ok := cm.hosts2partitions[h]; ok {
							cluster := fmt.Sprintf("%s-%s", c, p)
							for _, s := range cm.clusters[cluster].hosts2optimizers[h] {
								if o, ok := cm.clusters[cluster].optimizers[s]; ok {
									o.input <- m
									}
								}
							}
						}
					} else if mtype == lp.CCMSG_TYPE_EVENT {
						event := lp.CCEvent(m)
						if event.Name() == cm.config.JobEventName {
							var jdata ccspecs.BaseJob
							value := lp.GetEventValue(event)
							d := json.NewDecoder(strings.NewReader(value))
							d.DisallowUnknownFields()
							if err := d.Decode(&jdata); err == nil {
								if jdata.State == "running" {
									err = cm.NewJob(jdata)
								} else {
									err = cm.CloseJob(jdata)
								}
								if err != nil {
									cclog.ComponentError("ClusterManager", "Failed to process job", jdata.JobID, ":", err.Error())
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

	cm.done <- true

	for _, c := range cm.clusters {
		for ident, s := range c.optimizers {
			cclog.ComponentDebug("ClusterManager", "Send close to session", ident)
			s.optimizer.Close()
			close(s.input)
			close(s.output)
		}
	}
	cm.optWg.Wait()
	cclog.ComponentDebug("ClusterManager", "All sessions closed")
	<-cm.done
	cm.wg.Done()
	cclog.ComponentDebug("ClusterManager", "CLOSE")
}

func NewClusterManager(wg *sync.WaitGroup, configFile string) (ClusterManager, error) {
	cm := new(clusterManager)

	err := cm.Init(wg, configFile)
	if err != nil {
		return nil, err
	}

	return cm, err
}
