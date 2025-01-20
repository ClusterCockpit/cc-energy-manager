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
