# Standalone test for the node energy optimizer
The node energy optimizer modifies the node setting based on the policies defined by cluster manager and hardware measurement from the node.

The node agent receives hardware counter measurements and passes them onto the energy optimizer which calculates an optimal value for the node. The new value is then applied to the node and the details are sent to the cluster manager. This method is repeated every few seconds until the job terminates. Once the job has terminated the node agent cleanup process is started which gathers the information sends it to the cluster manager and returns the node to the default state.

## Configure the NATS server
First install the cc-metric-collector container, which is a NATS server that mimics the communication actions of ClusterCockpit.

To configure the server to publish and receiver messages on the NATS channels the following files need to be modified:

•	receivers.json
```
{
 "myhttp": {
  "type": "http",
  "port": "8000"
 },
 "mynats": {
  "type": "nats",
  "subject": "ccgeneral"
 }
}
```

•	sinks.json
```
{
    "mynats1": {
        "type": "nats",
        "host": "localhost",
        "port": "4222",
	"flush_delay" : "1s",
        "subject": "ccgeneral"
    }
}
```

•	collectors.json
```
{
    "loadavg": {},
    "likwid": {
    "force_overwrite" : false,
    "invalid_to_zero" : false,
    "liblikwid_path" : "/usr/lib/liblikwid.so",
    "accessdaemon_path" : "/usr/sbin/likwid-accessD",
    "access_mode" : "perf_event",
    "lockfile_path" : "/var/run/likwid.lock",
    "eventsets": [
      {     
        "events" : {
          "FIXC0": "INSTR_RETIRED_ANY",
          "PWR0": "PWR_PKG_ENERGY"
        },
        "metrics" : [
          {
            "name": "retired_instructions",
            "calc": "FIXC0",
            "publish": true,
            "type": "hwthread"
          },
          {
            "name": "pkg_energy",
            "calc": "PWR0",
            "publish": true,
            "type": "socket"
          }
        ]
      }
    ]
    }
}
```

•	config.json
```
{	
  "sinks": "/etc/cc-metric-collector/sinks.json",
  "collectors" : "/etc/cc-metric-collector/collectors.json",
  "receivers" : "/etc/cc-metric-collector/receivers.json",
  "router" : "/etc/cc-metric-collector/router.json",
  "interval": "5s",
  "duration": "1s"
}
```

To execute the NATS server container run the command:
```
singularity exec --bind $(pwd)/cc-metric-collector-config:/etc/cc-metric-collector cc-metric-collector.sif /usr/bin/cc-metric-collector -config "/etc/cc-metric-collector/config.json" -debug
```
Once the execution of the NATS server has been verified to produce and send the correct messages are being sent, we can proceed to test the optimizer.

## How to build

In the directory containing the test code execute the following command:
```
$ go build
```
This will produce an executable file called example.

## How to run
To execute the test optimizer type the command:
```
`$ ./example
```
