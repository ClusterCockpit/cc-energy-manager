// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type Aggregator interface {
	Add(m lp.CCMessage)
	Get() (map[string]float64, error)
}

func New(rawConfig json.RawMessage) Aggregator {
	var err error
	var ag Aggregator

	var cfg struct {
		Type string `json:"type"`
	}

	if err = json.Unmarshal(rawConfig, &cfg); err != nil {
		cclog.Warn("Error while unmarshaling raw config json")
		return nil
	}

	switch cfg.Type {
	case "last":
		ag, err = NewLastAggregator(rawConfig)
		// case "s3":
		// 	ar = &S3Archive{}
	default:
		cclog.Errorf("Unknown aggregator %s", cfg.Type)
	}

	return ag
}
