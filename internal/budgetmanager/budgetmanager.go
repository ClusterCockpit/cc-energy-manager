// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved. This file is part of cc-energy-manager.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package budgetmanager

type budgetManagerConfig struct {
	Budgets map[string]struct {
		MaxPowerBudget float64 `json:"max_power_budget"`
		MinPowerBudget float64 `json:"min_power_budget,omitempty"`
	} `json:"budgets"`
}

type budgetManager struct{}

func (b *budgetManager) CheckPowerBudget(cluster string, diff int) bool {
	// if cluster, ok := cm.clusters[cluster]; ok {
	// 	if cluster.maxBudget_watt == cluster.minBudget_watt && cluster.minBudget_watt == 0 {
	// 		return true
	// 	}
	// 	if diff >= 0 && cluster.curBudget_watt+float64(diff) < cluster.maxBudget_watt {
	// 		cluster.curBudget_watt += float64(diff)
	// 		return true
	// 	}
	// 	if diff < 0 && cluster.curBudget_watt+float64(diff) > cluster.minBudget_watt {
	// 		cluster.curBudget_watt += float64(diff)
	// 		return true
	// 	}
	// }

	return false
}
