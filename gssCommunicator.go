//package OptimizerTest

package main

import (
	//	"context"
	"fmt"
	"time"

	//	"log/slog"
	//	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

var retired_instruction_total int = 0
var pkg_energy_total float64 = 0.0

// initialize the start to zero
var start bool = false

var hostname string = ""
var output_message string = ""

// Variable for NATS subject that we listen to
var subject_receive string = ""

// Variable for NATS subject that we publish to likwid
var subject_publish_likwid string = ""

// Variable for NATS subject that we publish to likwid
var subject_publish_message string = ""

// variable to indicate if a job has started
var job_start bool = false

// variable to indictate if a job has stopped
var job_stop bool = false

var jobID string = "my_job_001"
var timestamp = ""

// create the job information communicator
func CreateJobInfoCommunicator(subject string) []string {
	// Connect to server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	// Channel Subscriber
	ch := make(chan *nats.Msg, 200)

	sub, err := nc.ChanSubscribe(subject, ch)
	if err != nil {
		fmt.Println("subscription to NATS channel error")
		sub.Unsubscribe()
		panic(err)
	}

	msg := <-ch

	// split the message block into individual lines
	var lines []string = regexp.MustCompile("\r?\n").Split(string(msg.Data), -1)

	// return the individual lines as strings
	return lines

}

// create the communicator from the input parameters subscription subject
// and returns the NATS messages as an array of strings
func CreateCommunicator(subject string) []string {
	// Connect to server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	// Channel Subscriber
	ch := make(chan *nats.Msg, 200)

	sub, err := nc.ChanSubscribe(subject, ch)
	if err != nil {
		fmt.Println("subscription to NATS channel error")
		sub.Unsubscribe()
		panic(err)
	}

	msg := <-ch

	// split the message block into individual lines
	var lines []string = regexp.MustCompile("\r?\n").Split(string(msg.Data), -1)

	return lines

}

func ParseNatsMessages(lines []string) {
	// get the number of lines in the nats message
	var max_lines int = len(lines)

	// loop through all the lines
	for i := 0; i < max_lines; i++ {
		if start {
			// Debug statements
			if debug {
				fmt.Println("if start is true : ", start)
			}
			if debug {
				fmt.Println(lines[i])
			}
			// get the lines that contain the retired instruction metrics
			if strings.Contains(lines[i], "retired_instructions") {
				ParseRetiredInstructions(lines[i])
			}
			// get the lines that contain the package metrics
			if strings.Contains(lines[i], "pkg_energy") {
				if strings.Contains(lines[i], "type-id=1") {
					ParsePackageEnergyEnd(lines[i])
				}
				ParsePackageEnergy(lines[i])
			}
		}
		if !start {
			if strings.Contains(lines[i], "proc_total") {
				start = true
				if debug {
					fmt.Println("begining of metric block used for the calculation")
				}
				// verify that the pkg_enery_total and retired_instruction_total values are zero
				if pkg_energy_total != 0 {
					if debug {
						fmt.Println("In the start function pkg_energy_total is not NULL. So set to NULL")
					}
					pkg_energy_total = 0.0
				}
				if retired_instruction_total != 0 {
					if debug {
						fmt.Println("In the start function retired_instruction_total is not NULL. So set to NULL")
					}
					retired_instruction_total = 0
				}
			}
		}
	}
}

// parse the line that contains the string retired instructions
func ParseRetiredInstructions(line string) {
	// retired instruction line tokens in the retired instruction message string
	var line_token_ri []string

	// the number of tokens in the retired instruction message string
	var max_token_ri int

	// split the message line on comma to get the string tokens
	line_token_ri = regexp.MustCompile(`[\s,]+`).Split(string(line), -1)

	// get the number of string tokens
	max_token_ri = len(line_token_ri)

	for i := 0; i < max_token_ri; i++ {
		if debug {
			fmt.Println("max_token_ri = ", max_token_ri, "i = ", i)
		}
		if debug {
			fmt.Println("The retired instruction tokens [", i, "] = ", line_token_ri[i])
		}
		// find the string token that contains the metric value
		if strings.Contains(line_token_ri[i], "value=") {
			ParseRetiredInstructionValues(line_token_ri[i])
		}
	}
}

// parse the retired instruction line to extract the value
func ParseRetiredInstructionValues(line string) {
	// the values of the token retired instruction containing the string value
	var value_token_ri []string

	// the number of sub-tokens in the retired instruction value token
	var max_value_ri int

	value_token_ri = regexp.MustCompile("value=").Split(string(line), -1)
	max_value_ri = len(value_token_ri)
	for i := 0; i < max_value_ri; i++ {
		if debug {
			fmt.Println("max_value_ri = ", max_value_ri, "i = ", i)
		}
		if debug {
			fmt.Println("The retired instruction value tokens [", i, "] = ", value_token_ri[i])
		}
		if value_token_ri[i] != "" {
			if debug {
				fmt.Println("The retired instruction value tokens [", i, "] that is not empty = ", value_token_ri[i])
			}
			ri_value, err := strconv.Atoi(value_token_ri[i])

			if err != nil {
				if debug {
					fmt.Println("Error converting retired instruction string value to int : ", err)
				}
			}
			if debug {
				fmt.Println("The retired instruction value is : ", ri_value)
			}
			retired_instruction_total += ri_value
			if debug {
				fmt.Println("The total retired instructions are : ", retired_instruction_total)
			}
		}
	}
}

// parse the line that contains the string pkg_energy
func ParsePackageEnergy(line string) {

	// package energy line tokens in the package energy message string
	var line_token_pkg []string

	// the number of tokens in the package energy message string
	var max_token_pkg int

	// split the message line on comma to get the string tokens
	line_token_pkg = regexp.MustCompile(`[\s,]+`).Split(string(line), -1)
	// get the number of string tokens
	max_token_pkg = len(line_token_pkg)

	for i := 0; i < max_token_pkg; i++ {
		if debug {
			fmt.Println("max_token_pkg = ", max_token_pkg, "i = ", i)
		}
		if debug {
			fmt.Println("The package energy tokens [", i, "] = ", line_token_pkg[i])
		}
		// find the string token that contains the metric value
		if strings.Contains(line_token_pkg[i], "value=") {
			ParsePackageEnergyValues(line_token_pkg[i])
		}
	}
}

// parse the package energy line to extract the value
func ParsePackageEnergyValues(line string) {
	// the values of the token package energy containing the string value
	var value_token_pkg []string

	// the number of sub-tokens in the package energy  value token
	var max_value_pkg int

	// split the message line into individual tokens
	value_token_pkg = regexp.MustCompile("value=").Split(string(line), -1)
	max_value_pkg = len(value_token_pkg)

	// iterate threough the tokens to find the value of the package energies
	for i := 0; i < max_value_pkg; i++ {
		if debug {
			fmt.Println("max_value_pkg = ", max_value_pkg, "i = ", i)
		}
		if debug {
			fmt.Println("The package energy value tokens [", i, "] = ", value_token_pkg[i])
		}
		if value_token_pkg[i] != "" {
			if debug {
				fmt.Println("The package value [", i, "] that is not empty = ", value_token_pkg[i])
			}
			pkg_energy_value, err := strconv.ParseFloat(value_token_pkg[i], 64)
			if err != nil {
				if debug {
					fmt.Println("Error converting pkg energy string value to float64 : ", err)
				}
			}
			if debug {
				fmt.Println("The package energy value is : ", pkg_energy_value)
			}
			pkg_energy_total += pkg_energy_value
			if debug {
				fmt.Println("The total package energy is : ", pkg_energy_total)
			}
		}
	}
}

// parse the lines to find the last line of a data message block
func ParsePackageEnergyEnd(line string) {
	// package energy line tokens in the package energy message string
	var line_token_pkg []string

	// the number of tokens in the package energy message string
	var max_token_pkg int

	// split the message line into individual tokens
	line_token_pkg = regexp.MustCompile(`[\s,]+`).Split(string(line), -1)
	// get the number of string tokens
	max_token_pkg = len(line_token_pkg)

	// iterate threough the tokens to find the value of the last package energies
	for i := 0; i < max_token_pkg; i++ {
		if debug {
			fmt.Println("max_token_pkg = ", max_token_pkg, "i = ", i)
		}
		if debug {
			fmt.Println("The package energy tokens [", i, "] = ", line_token_pkg[i])
		}
		if strings.Contains(line_token_pkg[i], "hostname=") {
			ParseHostname(line_token_pkg[i])
		}

		// find the string token that contains the metric value
		if strings.Contains(line_token_pkg[i], "value=") {
			ParsePackageEnergyValues(line_token_pkg[i])
		}
	}

	fmt.Println("end of metric block used for the calculation")
	// send the retired instuctions and package enegry values to the optimizer
	// Send values to the optimizer
	SetRetiredInstructions(retired_instruction_total)
	SetPackageEnergy(pkg_energy_total)

	// set a default fudge factor based on the processor
	fudge_factor = SetFudgeFactor(110)

	// calculate the energy delay product
	edp = CalculateEDP(fudge_factor, power_per_socket, retired_instr)

	if debug {
		fmt.Println("The powercap value before UpdateGSS = ", powercap)
	}

	fmt.Println("test printing of GSS in parse function:")

	if g == nil {
		fmt.Println("pointer g is nil in the parser function")
	}

	// calculate the new powercap value to be sent to the node
	powercap = UpdateGSS(g, powercap, edp)

	// if the hostname has been set publish the message to the publishing subject
	if hostname != "" {

		// create the output message in the format hostname=, powercap=
		output_message_likwid := fmt.Sprintf("hostname=%s, powercap=%d", hostname, powercap)

		// create a timestamp for when the new powercap value is published
		t := time.Now()
		// Use the time format RFC3339
		timestamp = t.Format(time.RFC3339)
		// create the output message in the format jobID=, hostname=, powercap=, total_package_energy=, total_retired_instructions=, timestamp=
		output_message = fmt.Sprintf("jobID=%s, hostname=%s, powercap=%d, total_package_energy=%f, total_retired_instructions=%d, timestamp=%s", jobID, hostname, powercap, pkg_energy_total, retired_instruction_total, timestamp)
		// send data to modify the processors powercap along with information required to recreate the optimizer result
		PublishMessage(output_message_likwid, subject_publish_likwid, output_message, subject_publish_message)

	}

	// reset the pkg_enery_total and retired_instruction_total
	pkg_energy_total = 0.0
	retired_instruction_total = 0
	// reset the start flag to enable the next block to be processed
	start = false
}

// parse the line to extract the hostname
func ParseHostname(line string) {
	var hostname_token_pkg []string

	// the number of sub-tokens in the package energy hostname token
	var max_hostname_pkg int
	// split the message line into individual tokens
	hostname_token_pkg = regexp.MustCompile("hostname=").Split(string(line), -1)
	max_hostname_pkg = len(hostname_token_pkg)

	// iterate threough the tokens to find the value of the hostname/node
	for i := 0; i < max_hostname_pkg; i++ {
		if debug {
			fmt.Println("The max_hostname_pkg[", i, "] = ", hostname_token_pkg[i])
		}
		if hostname_token_pkg[i] != "" {
			if debug {
				fmt.Println("The hostname is not empty = ", hostname_token_pkg[i])
			}
			hostname = hostname_token_pkg[i]
		}
	}
}

// publish the message containing the calculated powercap back to the NATS server
func PublishMessage(message_likwid string, subject_likwid string, message_cc string, subject_cc string) {
	// connect to the NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	// verify the connection
	if err != nil {
		panic(err)
	}
	// Publish the message to the subject of the NATS channel to modify the nodes powercap
	err = nc.Publish(subject_likwid, []byte(message_likwid))
	if err != nil {
		panic(err)
	}
	// Publish the message to the subject of the NATS channel to CC
	err = nc.Publish(subject_cc, []byte(message_cc))
	if err != nil {
		panic(err)
	}
}
