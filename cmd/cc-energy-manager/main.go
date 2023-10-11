// Copyright (C) 2023 NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package main

import (
	"fmt"

	"github.com/ClusterCockpit/cc-energy-manager/internal/api"
	"github.com/ClusterCockpit/cc-metric-collector/collectors"
)

var likwid_collector collectors.LikwidMetric

func main() {

	if err := api.ApiPrinter("Bla"); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
