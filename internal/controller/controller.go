// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package controller

import (
	"encoding/json"
	"fmt"
	"time"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type Controller interface {
	Set(key string, v int)
}

type ccControllerConfig struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ccController struct {
	name   string
	Type   string // TODO: Find suitable private name
	output chan lp.CCMessage
}

func New(rawConfig json.RawMessage) Controller {
	var c Controller

	return c
}

func NewCcController(rawConfig json.RawMessage) (*ccController, error) {
	var err error
	c := &ccController{}

	var cfg ccControllerConfig

	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return nil, err
	}
	// TODO: Finish initialization and decide on data structure

	return c, nil
}

func (c *ccController) AddOutput(output chan lp.CCMessage) {
	c.output = output
}

func (c *ccController) Set(key string, v int) {
	out, err := lp.NewPutControl(c.name, map[string]string{
		"hostname": key,
		"type":     c.Type,
		"type-id":  "0",
	}, nil, fmt.Sprintf("%d", v), time.Now())
	if err != nil {
		cclog.ComponentError("Controller", out.String(), err.Error())
		return
	}

	c.output <- out
}
