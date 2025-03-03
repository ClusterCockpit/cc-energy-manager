// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package hostdb

import (
	"encoding/json"
	"fmt"

	// topo "github.com/ClusterCockpit/cc-node-controller/pkg/ccTopology"
	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	"github.com/nats-io/nats.go"
)

type HostDBConfig struct {
	NatsSendSubject string `json:"nats_send_subject"`
	NatsRecvSubject string `json:"nats_recv_subject"`
	NatsServer      string `json:"nats_server"`
	NatsPort        string `json:"nats_port"`
}

type HostDBHostControlsEntry struct {
	Category    string `json:"category"`
	Name        string `json:"name"`
	DeviceType  string `json:"device_type"`
	Description string `json:"description"`
	Methods     string `json:"methods"`
}

type HostDBHostTopology struct {
	// HWthreads []topo.HwthreadEntry `json:"hwthreads"`
	// CpuInfo   topo.CpuInformation  `json:"cpu_info"`
}

type HostDBHost struct {
	Hostname string                    `json:"hostname"`
	Controls []HostDBHostControlsEntry `json:"controls"`
	Topology HostDBHostTopology        `json:"topology"`
}

type hostDB struct {
	hosts  map[string]HostDBHost
	config HostDBConfig
	conn   *nats.Conn
}

type HostDB interface {
	Init(config json.RawMessage) error
	AddHost(hostname string) error
	GetHostControls(hostname string) []HostDBHostControlsEntry
	GetHostTopology(hostname string) HostDBHostTopology
	Close()
}

func (db *hostDB) Init(config json.RawMessage) error {
	if len(config) > 0 {
		var conf HostDBConfig
		err := json.Unmarshal(config, &conf)
		if err != nil {
			err := fmt.Errorf("failed to read configuration for HostDB: %v", err.Error())
			cclog.ComponentError("HostDB", err.Error())
			return err
		}
		db.config = conf
	}
	server := fmt.Sprintf("nats://%s", db.config.NatsServer)
	if len(db.config.NatsPort) > 0 {
		server += fmt.Sprintf(":%s", db.config.NatsPort)
	}
	conn, err := nats.Connect(server)
	if err != nil {
		err = fmt.Errorf("failed to connect to NATS server %s: %v", server, err.Error())
		cclog.ComponentError("HostDB", err.Error())
		return err
	}
	db.conn = conn
	return nil
}

func (db *hostDB) AddHost(hostname string) error {
	return nil
}

func (db *hostDB) GetHostControls(hostname string) []HostDBHostControlsEntry {
	return []HostDBHostControlsEntry{}
}

func (db *hostDB) GetHostTopology(hostname string) HostDBHostTopology {
	return HostDBHostTopology{}
}

func (db *hostDB) Close() {
	if db.conn != nil {
		db.conn.Close()
	}
}

// func GetControls(hostname string) ([]HostDBHostControlsEntry, error) {
// 	var controls []HostDBHostControlsEntry
// 	var wg sync.WaitGroup
// 	cclog.ComponentDebug("HostDB", "Get controls for host", hostname)
// 	//fmt.Sprintf("controls,hostname=%s,method=GET,type=node,type-id=0 value=0.0", hostname),
// 	msg, err := lp.NewGetControl("controls", map[string]string{"hostname": hostname, "type": "node"}, map[string]string{}, time.Now())
// 	if err != nil {
// 		err = fmt.Errorf("failed to create control message: %v", err.Error())
// 		cclog.ComponentError("HostDB", err.Error())
// 		return controls, err
// 	}
// 	conn, err := nats.Connect(nats.DefaultURL)
// 	if err != nil {
// 		err = fmt.Errorf("failed to connect to NATS: %v", err.Error())
// 		cclog.ComponentError("HostDB", err.Error())
// 		return controls, err
// 	}
// 	defer conn.Close()

// 	cclog.ComponentError("HostDB", "Sending controls request to", hostname)
// 	err = conn.Publish("controls", []byte(msg.String()))
// 	if err != nil {
// 		err = fmt.Errorf("failed to publish controls request: %v", err.Error())
// 		cclog.ComponentError("HostDB", err.Error())
// 		return controls, err
// 	}

// 	wg.Add(1)
// 	_, err = conn.Subscribe("response", func(msg *nats.Msg) {
// 		var c []HostDBHostControlsEntry
// 		err = json.Unmarshal(msg.Data, &c)
// 		if err != nil {
// 			cclog.ComponentError("HostDB", "Failed to parse controls response JSON from", hostname, ":", err.Error())
// 		} else {
// 			controls = c
// 		}
// 		wg.Done()
// 	})
// 	wg.Wait()

// 	return controls, nil
// }

// func GetTopology(hostname string) (HostDBHostTopology, error) {
// 	cclog.ComponentDebug("HostDB", "Get topology for host", hostname)
// 	return HostDBHostTopology{}, nil
// }

// func AddHost(hostname string) error {
// 	if _, ok := global_host_db.hosts[hostname]; !ok {
// 		cclog.ComponentDebug("HostDB", "Adding host", hostname)
// 		dbh := HostDBHost{
// 			Hostname: hostname,
// 			Controls: []HostDBHostControlsEntry{},
// 			Topology: HostDBHostTopology{},
// 		}

// 		c, err := GetControls(hostname)
// 		if err != nil {
// 			return err
// 		}
// 		t, err := GetTopology(hostname)
// 		if err != nil {
// 			return err
// 		}
// 		dbh.Controls = c
// 		dbh.Topology = t
// 		global_host_db.hosts[hostname] = dbh
// 		return nil
// 	}
// 	return fmt.Errorf("host %s already in DB", hostname)
// }

// func GetHostControls(hostname string) ([]HostDBHostControlsEntry, error) {
// 	if dbh, ok := global_host_db.hosts[hostname]; ok {
// 		return dbh.Controls, nil
// 	}
// 	return []HostDBHostControlsEntry{}, fmt.Errorf("host %s not in database", hostname)
// }
