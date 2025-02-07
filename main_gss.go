//package OptimizerTest

package main

import (
	"fmt"
	"time"
)

const debug = false

// define and initialize the EDP value to -1 as a default
var edp float64 = -1

// define and initialize the fudge factor value to 0 as a default
var fudge_factor int = 0

// define a pointer to the GSS datastructure and initialize it to NULL
var g *GSS = nil

// number of GSS optimizations done per time step
var optimization_number int = 10

// the time interval between each GSS update
var time_step_seconds time.Duration = 300

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

	/* duplicate
	// set the nats subscription subject to listen to
	subject_receive_job = "cc_job"
	*/

	// set the nats subscription subject to listen to
	subject_receive = "ccgeneral"

	// set the nats subscription subject to publish to likwid
	subject_publish_likwid = "ee-hpc-nats"

	// set the nats subscription subject to publish to CC
	subject_publish_message = "ee-hpc-nats-cc"

	if debug {
		fmt.Println("Initial job_start status = ", job_start)
		fmt.Println("Initial job_start status = ", job_stop)
	}
	/*
		// Don't need this code except for debugging
		// Currently the job_lines is a dummy setup and requires the start mechanism to be available
		job_lines := CreateJobInfoCommunicator(subject_receive_job)
		// print the number of lines in the message
		if debug {
			fmt.Println("number of lines in the job message = ", len(job_lines))
		}
	*/

	// continually calculate the GSS updates
	for {
		// the inner loop is where the NATS messages are processed
		for i := 0; i < optimization_number; i++ {
			// store the lines of data sent from the NATS server into an array of strings
			lines := CreateCommunicator(subject_receive)

			// iterate though the NATS message lines and process the message line data
			for j := 0; j < len(lines); j++ {
				// print the number of lines in the message
				if debug {
					fmt.Println("number of lines = ", len(lines))
				}

				// parsing the individual lines of the NATS server message
				ParseNatsMessages(lines)
			}
		}
		// sleep for the specified GSS interval (system policy)
		time.Sleep(time_step_seconds * time.Second)
	}

	// Unsubscribe
	// sub.Unsubscribe()
}
