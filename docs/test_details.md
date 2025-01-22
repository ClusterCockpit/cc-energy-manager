# Overview

## Communication Format

### Input message
The input message containing the node input data is sent on the NATS channel :
```ccgeneral```.

The two message lines before the start of the data block is : 

``` [WRITE Name: proc_run, Tags: map[hostname:o184i170 type:node], Meta: map[source:NatsReceiver(mynats)], fields: map[value:2], Timestamp: 0] ```

``` [WRITE Name: proc_total, Tags: map[hostname:o184i170 type:node], Meta: map[source:NatsReceiver(mynats)], fields: map[value:820], Timestamp: 0] ```

The input format of the message line that triggers the start of the data collection process is :

``` [WRITE Name: proc_total, Tags: map[hostname:o184i170 type:node], Meta: map[source:NatsReceiver(mynats)], fields: map[value:820], Timestamp: 0] ```

The next step is to parse and extract the values from the message lines containing the retired instruction and package energies : 

Retired instruction line format is :

```Name: retired_instructions, Tags: map[hostname:o184i170 type:hwthread type-id:29], Meta: map[source:NatsReceiver(mynats)], fields: map[value:96020], Timestamp: 1```

AND

Package energy line format is :

```Name: pkg_energy, Tags: map[hostname:o184i170 type:socket type-id:1], Meta: map[source:NatsReceiver(mynats)], fields: map[value:9.70745849609375], Timestamp: 1```


Once we have parsed and extracted data from the package energy & retired instruction lines associated with block. We calculate the EDP and reset the retired instruction and package energy valuees. Then continue parsing the next block of data and calculate the EDP.

### Output to update the node devices
The output message to update the hardware device values is sent to the NATS channel :
```ee-hpc-nats```.

The output message format used for updating the node hardware devices is :

```hostname=%s, powercap=%d```

Where ```%s``` is a string. ```%d``` is a integer

### Output debug monitor message
The output message used for debugging and monitoring is sent on the NATS channel : 
```ee-hpc-nats-c```.

The output message format used for debugging and monitoring is :

```jobID=%s, hostname=%s, powercap=%d, total_package_energy=%f, total_retired_instructions=%d, timestamp=%s```

Where ```%s``` is a string. ```%d``` is a integer, ```%f``` is a float

## Subscribing to the desired NATS channel
In the main function the following function call ``` lines := CreateCommunicator(subject_receive) ``` subscribes to the ccgeneral NATS channel ``` subject_receive = "ccgeneral" ```.

```
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
```

The ``` CreateCommunicator(subject_receive) ``` is a dummy function, which takes the name of the NATS channel as a parameter and returns the message lines as an array of string. This function needs to be modified because or replicated functionality with the function ``` func CreateCommunicator(subject string) []string ```.

## Main processing loop
To ensure that the messages are being contimually processed at a freuency defined by the system we encapsulate the message reading, parsing, GSS optimization calculations and sending the output messages used to update the node hardware settings.
```
// continually calculate the GSS updates
	for {
		// the inner loop is where the NATS messages are processed
		for i := 0; i < optimization_number; i++ {
			// store the lines of data sent from the NATS server into an array of strings
			lines := CreateCommunicator(subject_receive)

			// print the number of lines in the message
			if debug {
				fmt.Println("number of lines = ", len(lines))
			}

			// parsing the individual lines of the NATS server message
			ParseNatsMessages(lines)
		}
		// sleep for the specified GSS interval (system policy)
		time.Sleep(time_step_seconds * time.Second)
	}
```

## Parsing the incoming NATS messages
After subscribing to the ```ccgeneral``` NATS channel, we need to extract the retired instructions and package energy values for the specific nodes. To do this we need to parse the incoming messages.

The function ``` ParseNatsMessages(lines) ```, which takes an array of lines as the input parameter.
```
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
```

### 
