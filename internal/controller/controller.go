// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package controller

import (
	"encoding/json"
	"fmt"
	"time"
	"sync"
	"strings"
	"strconv"

	cccontrol "github.com/ClusterCockpit/cc-node-controller/pkg/ccControlClient"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	ccspecs "github.com/ClusterCockpit/cc-lib/schema"
)

var (
	Instance Controller
)

type Controller interface {
	Set(cluster string, deviceString string, control string, value string) error
	GetDeviceIdsForResources(cluster string, deviceType string, resource *ccspecs.Resource) []string
	Cleanup()
}

type ccControllerConfig struct {
	Nats cccontrol.NatsConfig `json:"nats"`
	ToposMaxAge int64 `json:"toposMaxAge"`
}

type ccController struct {
	/* Each CCControlClient is bound do a single subject.
	 * Since we need a subject per cluster, instantiate one CCControlClient
	 * for each subject (thus cluster). */
	controlClientsMutex sync.Mutex
	controlClients map[string]cccontrol.CCControlClient

	/* Map of hostname to topology. Cache previously requested toplogies to
	 * avoid roundtrips to cc-node-controller. */
	toposMutex sync.Mutex
	toposLastClear time.Time
	toposMaxAge time.Duration
	topos map[string]cccontrol.CCControlTopology

	nats cccontrol.NatsConfig
}

func NewCcController(rawConfig json.RawMessage) (*ccController, error) {
	var err error
	c := &ccController{}

	cfg := ccControllerConfig{
		ToposMaxAge: 60 * 60 * 24 * 1, // default of 1 day
	}

	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return nil, err
	}

	c.nats = cfg.Nats
	c.toposMaxAge = time.Duration(cfg.ToposMaxAge) * time.Second

	return c, nil
}

func (c *ccController) Set(cluster string, deviceString string, control string, value string) error {
	/* e.g. "node01/gpu/00000000:00:3c.0" --> "node01", "gpu", "00000000:00:3c.0" */
	deviceStringComponents := strings.Split(deviceString, "/")
	if len(deviceStringComponents) != 3 {
		// This case should not occur if we pass parameters with the right format
		cclog.Fatal("Invalid argument: Cannot decompose device string: '%s'", deviceString)
	}

	hostname := deviceStringComponents[0]
	deviceType := deviceStringComponents[1]
	deviceId := deviceStringComponents[2]

	controlClient, err := getControlClient(c, cluster)
	if err != nil {
		cclog.Error("getControlClient() failed: %v", err)
		return err
	}

	err = controlClient.SetControlValue(hostname, control, deviceType, deviceId, value)
	if err != nil {
		cclog.Warn("Setting control '%s' on host '%s' on cluster '%s' to value '%s' failed: %v", control, hostname, cluster, value)
	}

	/* If setting a control fails, this is non fatal for cc-energy-manager's execution. Emitting
	 * a warning should be enough in case one of the cc-node-controllers doesn't respond properly.
	 * An error should only be caused if:
	 * - cc-node-controller is broken
	 * - NATS problem
	 * - we are passing an illegal combination of parameters to controlClient.Set() */
	return nil
}

func getControlClient(c *ccController, cluster string) (cccontrol.CCControlClient, error) {
	c.controlClientsMutex.Lock()
	defer c.controlClientsMutex.Unlock()
	if _, ok := c.controlClients[cluster]; !ok {
		/* If we don't have a CCControlClient for the required cluster in our map, create a new one */
		cclog.Debug("No CCControlClient found for cluster %s. Creating new one", cluster)

		// TODO we have to set our appropriate NATS subject here
		controlClient, err := cccontrol.NewCCControlClient(c.nats)
		if err != nil {
			return nil, fmt.Errorf("NewCCControlClient failed: %w", err)
		}

		c.controlClients[cluster] = controlClient
	}

	return c.controlClients[cluster], nil
}

func (c *ccController) Cleanup() {
	c.controlClientsMutex.Lock()
	for _, controlClient := range c.controlClients {
		controlClient.Close()
	}
	clear(c.controlClients)
	c.controlClientsMutex.Unlock()
}

func (c *ccController) GetDeviceIdsForResources(cluster string, deviceType string, resource *ccspecs.Resource) []string {
	if deviceType == "socket" {
		sockets, err := hwthreadsToSockets(c, cluster, resource.Hostname, resource.HWThreads)
		if err != nil {
			cclog.Errorf("Unable to convert hwthreads to sockets: %v", err)
			return make([]string, 0)
		}
		return sockets
	} else if deviceType == "nvidia_gpu" {
		return resource.Accelerators
	} else {
		cclog.Fatal("GetDeviceIdsForResources: Unsupported device '%s'. Please fix the configuration")
		return nil
	}
}

func hwthreadsToSockets(c *ccController, cluster string, host string, hwthreads []int) ([]string, error) {
	/* Returns a list of sockets belonging to specified hardware threads.
	 * Values are returned as strings for convenience, since we never really need
	 * them as integers. */

	topo, err := getTopoForHost(c, cluster, host)
	if err != nil {
		return nil, err
	}

	sockets := make(map[int]bool)
	for _, hwthread := range hwthreads {
		for _, cpuData := range topo.HWthreads {
			if hwthread == cpuData.CpuID {
				sockets[cpuData.Socket] = true
			}
		}
	}

	results := make([]string, 0)
	for socket, _ := range sockets {
		results = append(results, strconv.Itoa(socket))
	}

	return results, nil
}

func getTopoForHost(c *ccController, cluster string, hostname string) (*cccontrol.CCControlTopology, error) {
	curTime := time.Now()
	if c.toposLastClear.Add(c.toposMaxAge).Before(curTime) {
		cclog.Debug("Clearing node topology cache")
		clear(c.topos)
		c.toposLastClear = curTime
	}

	ccControlClient, err := getControlClient(c, cluster)
	if err != nil {
		return nil, err
	}

	c.toposMutex.Lock()
	defer c.toposMutex.Unlock()

	if _, ok := c.topos[hostname]; !ok {
		topo, err := ccControlClient.GetTopology(hostname)
		if err != nil {
			return nil, fmt.Errorf("GetTopology() failed: %w", err)
		}

		c.topos[hostname] = topo
	}

	/* If we get here, the value must exist in the map. */
	topo, _ := c.topos[hostname]
	return &topo, nil
}
