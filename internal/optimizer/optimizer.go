// Copyright (C) 2023 NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package optimizer

import (
	"math"
	"strings"
	"encoding/json"
    "fmt"
    "os"
	"io/ioutil"
)






// a datastructure to store the nodes power variable
type Node_Power_Time struct {
	node int
	power float64
	previous_power float64
	time int64
	previous_time int64
	edp float64
}

func CreateNodePowerTime (node int, power float64, previous_power float64, time int64, previos_time int64, edp float64) Node_Power_Time {
	tmp := Node_Power_Time{node, power, previous_power, time, previos_time, edp}
	return tmp
}

// create a map datasrtucture that with the node ID as the key and value the nodes location in the node data array
NodeMap := make(map[int] Node_Power_Time)

// add a node to the map datastructure, if the node exists update the data
func AddToMap(npt Node_Power_Time node_id int){
	NodeMap[node_id] = npt
}

// delete an item from the map datastructure
func RemoveFromMap(node_id int) {
	// the delete function doesn't return a value
	delete(NodeMap, node_id)
}

// using the node ID to find the location of node data item in the map
func FindNodeData(node_id int) Node_Power_Time {
	var data Node_Power_Time
	if data, found := NodeMap[node_id]; found {
		fmt.Println("item found", data)
		return data
	}
	fmt.Println("Item not found for node ", node_id , " in the map datastructure \n")
}

// sum up edp from all the nodes for a specific job
func SumUpEDP() float64 {
	var total_edp float64

	for _, Node_Power_Time := NodeMap {
		total_edp += Node_Power_Time.edp
	}
	return total_edp
}









// old function that creates a GSS data structure
/*func CreateGSS(tuning_lower_outer_border, tuning_lower_inner_border, tuning_upper_inner_border, tuning_upper_outer_border int, metric_lower_outer_border, metric_lower_inner_border, metric_upper_inner_border, metric_upper_outer_border float64, mode Mode, limits Limits) GSS {
	var golden GSS = GSS{tuning_lower_outer_border, tuning_lower_inner_border, tuning_upper_inner_border, tuning_upper_outer_border, metric_lower_outer_border, metric_lower_inner_border, metric_upper_inner_border, metric_upper_outer_border, mode, limits}
	return golden
}

// old function that generates a new GSS object whose parameters are initialized to zero
func InitializeGSS(limit Limits) GSS {
	gss := CreateGSS(0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, NarrowDown, limit)
}
*/
// calculate the EDP for all the nodes in the job
func CalculateTotalEDP() float64 {
	total_edp float64 = 0.0
	// Iterating map using for range loop
    for id, npt := range Node_Power_Time {
        fmt.Println(id, npt)
		total_edp += npt.edp
		fmt.Println(total_edp)
    }
	return total_edp
}

