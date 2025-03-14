// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	"encoding/json"
	"fmt"
	"math"

	cclog "github.com/ClusterCockpit/cc-lib/ccLogger"
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type Aggregator interface {
	Add(m lp.CCMessage)
	Get() map[string]float64
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
		ag, _ = NewLastAggregator(rawConfig)
	default:
		cclog.Errorf("Unknown aggregator %s", cfg.Type)
	}

	return ag
}

func valueToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	}
	return math.NaN(), fmt.Errorf("cannot convert %v to float64", value)
}
