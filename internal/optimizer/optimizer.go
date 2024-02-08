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
	run_timetime int64
	edp float64
	ed2p float64
}

func CreateNodePowerTime (node int, power float64, runtime int64, edp float64, ed2p float64) Node_Power_Time {
	tmp := Node_Power_Time{node, power, runtime, edp, ed2p}
	return tmp
}

// create a map datasrtucture that with the node ID as the key and value the nodes location in the node data array
NodeMap := make(map[int] Node_Power_Time)

// add a node to the map datastructure, if the node exists update the data
func AddToMap(npt Node_Power_Time, node_id int){
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
// TODO: change the name to include all nodes
func SumUpEDP() float64 {
	var total_edp float64 = 0.0

	for _, Node_Power_Time := NodeMap {
		total_edp += Node_Power_Time.edp
	}
	return total_edp
}

// sum up ed2p from all the nodes for a specific job
func SumUpED2P() float64 {
	var total_ed2p float64 = 0.0

	for _, Node_Power_Time := NodeMap {
		total_ed2p += Node_Power_Time.ed2p
	}
	return total_ed2p
}

// sum up ed2p from all the nodes for a specific job
func SumUpEDPED2P() (float64, float64) {
	var total_edp float64 = 0.0
	var total_ed2p float64 = 0.0

	for _, Node_Power_Time := NodeMap {
		total_edp += Node_Power_Time.edp
		total_ed2p += Node_Power_Time.ed2p
	}
	return total_edp, total_ed2p
}

// calculate the EDP for all the nodes in the job
func CalculateTotalJobEDP(job_arr []int) float64 {
	var total_edp float64 = 0.0
	var node_id int = 0
	// Iterating through the nodes in the job and extracting the edp value from thev map
	for _, node_id := range job_arr {
		npt := FindNodeData(node_id)
		total_edp += npt.edp
		fmt.println(total_edp)
	  }

	return total_edp
}

// calculate the both the EDP and ED2P for all the nodes in the job
// TODO: record data for an audit report
func CalculateTotalJobED2P(job_arr []int) float64 {
	var total_ed2p float64 = 0.0
	// Iterating through the nodes in the job and extracting the edp2 value from thev map
	for i, node_id := range job_arr {
		npt := FindNodeData(node_id)
		total_ed2p += npt.ed2p
		fmt.println(total_ed2p)
	  }

	return total_ed2p
}

// calculate the EDP for all the nodes in the job
func CalculateTotalJobEDPED2P(job_arr []int) (float64, float64) {
	var total_edp float64 = 0.0
	var total_ed2p float64 = 0.0
	// Iterating through the nodes in the job and extracting the edp value from thev map
	for _, node_id := range job_arr {
		npt := FindNodeData(node_id)
		total_edp += npt.edp
		total_ed2p += npt.ed2p
		fmt.println(total_edp)
		fmt.println(total_ed2p)
	  }

	return total_edp, total_ed2p
}

// TODO: analysis node behaviour over time and record the information
// TODO: look at nodes over time and update the fudge factor based on the updated data
