// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package aggregator

import (
	lp "github.com/ClusterCockpit/cc-lib/ccMessage"
)

type Aggregator interface {
	Add(m lp.CCMessage)
	Get() (map[string]float64, error)
}
