//package OptimizerTest

package main

import "fmt"

const debug = false

var edp float64 = -1
var fudge_factor int = 0
var g *GSS = nil

func main() {

	// Initalize the GSS data structure
	if debug {
		fmt.Println("Initializing the GSS datastructure")
	}
	// g is a GSS pointer
	g = new(GSS)
	// Set initial the GSS values in the GSS pointer g
	g.InitGSS()

	// Print out GSS values
	g.PrintGSSValues()

	if debug {
		fmt.Println("Before calling the function SetFudgeFactor")
	}

	// call the function SetFudgeFactor
	fudge_factor = SetFudgeFactor(110)
	if debug {
		fmt.Println("After calling the function SetFudgeFactor")
	}

	// initialize the EDP value to -1 as a default
	edp = -1

	// set the nats subscription subject to listen to
	subject_receive = "ccgeneral"

	// set the nats subscription subject to publish to likwid
	subject_publish_likwid = "ee-hpc-nats"

	// set the nats subscription subject to publish to CC
	subject_publish_message = "ee-hpc-nats-cc"

	// store the lines of data sent from the NATS server into an array of strings
	lines := CreateCommunicator(subject_receive)

	// print the number of lines in the message
	if debug {
		fmt.Println("number of lines = ", len(lines))
	}

	// parsing the individual lines of the NATS server message
	ParseNatsMessages(lines)

	// Unsubscribe
	// sub.Unsubscribe()
}
