// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"encoding/json"
	"fmt"
)

type Optimizer interface {
	Start(float64) (float64, bool)
	Update(float64) float64
}

func NewOptimizer(rawConfig json.RawMessage) (Optimizer, error) {
	var cfg struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("unable to parse optimizer config: %v", err)
	}

	switch cfg.Type {
	case "gss":
		return NewGssOptimizer(rawConfig)
	case "trace":
		return NewTraceOptimizer(rawConfig)
	case "test":
		return NewTestOptimizer(rawConfig)
	case "wqr":
		return NewWQROptimizer(rawConfig)
	default:
		return nil, fmt.Errorf("invalid/unsupported optimizer type: %s", cfg.Type)
	}
}
