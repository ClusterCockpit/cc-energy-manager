# cc-energy-manager

## Overview

[![Build](https://github.com/ClusterCockpit/cc-energy-manager/actions/workflows/test.yml/badge.svg)](https://github.com/ClusterCockpit/cc-energy-manager/actions/workflows/test.yml)

*cc-energy-manager* is a daemon, which adjusts powerlimits on an HPC cluster to automatically optimize energy consumption.
It is part of the [ClusterCockpit ecosystem](https://clustercockpit.org/).

## Problem and Motivation

With large HPC systems power draw becomes an ever growing issue for grid infrastructure and its environmental footprint.
If it is possible to reduce this power draw without affecting the powerformance, this is very welcoming.

To set the context: All modern CPUs and GPUs have integrated power management, which tries to balance maximum clock speeds, maximum power draw, and maximum temperature.
Most of the time systems will run at whatever those parameters are set by the system or chip vendor.
However, on many systems it is also possible to lower those limits (i.e. the opposite of "overclocking").

For example, if a CPU normally operates at a maximum power of 300W, we could lower the power limit to e.g. 150W.
Of course this will have an impact on how fast the CPU can run.
The integrated power management will have to lower clock speeds in order to fulfill the maximum allowed power.
The real power draw may not be 100% exactly match what was actually requested (depending on the chip).
However it works well enough that one can usually expect the power usage to decrease when lowering the limit.

Because the power management will have to reduce clock speeds to fulfill the requested power constraints, performance will decrease.
However, what can be observed is that the performance will not decrease proportionally to the reduction of power usage, thus increasing the run-to-completion efficiency.
On consumer chips this behavior is well known, when chips operate at the upper edge of their limits.
However, even on lower clocked chips an efficiency improvements can also be observed, since the chip may reduce clock speeds of functional blocks, which are not highly utilized.

Assuming these fundamentals, we can dynamically adjust the power not just on one machine, but on an entire HPC cluster.
This is exactly what cc-energy-manager tries to do.

## How it works

We previously described that efficiency improvements are possible.
However, this poses a few challenges for the implementation.
For example, in order to performance some type of optimization, we need to be able to do the following things:

- Determine the power draw of a chip
- Change the power limit of a chip
- Somehow determine how the performance of an application is affected after the power limit was changed.

Especially the last point is not trivial.
While it is possible to instrument code to report performance, this has to be done for every bit of code being run on the cluster.
This will of course depend on the exact circumstances, but for a many-user system, this cannot easily be done.
So another way of determining performance is needed.
cc-energy-manager does this via a *performance proxy*, which is just a number that we somehow calculate to determine the overall code performance.

Without knowing the code running on a machine, one such a performance proxy is the instruction rate (i.e. how many instructions per second a CPU executes).
While there are exceptions (e.g. spinlocks), the instruction rate will usually be proportional to the performance of the executed code.
This is something that we can observe using tools like [LIKWID](https://github.com/RRZE-HPC/likwid).

In cc-energy-manager we use [cc-metric-collector](https://github.com/ClusterCockpit/cc-metric-collector) to get those performance statistics of our HPC cluster.
[cc-node-controller](https://github.com/ClusterCockpit/cc-node-controller) is used to actually change the power limits.
cc-energy-manager will then perform a Golden-section search based optimization to find the minimum *EDP* (Energy Delay Product).
While the EDP is not the most optimum energy-to-completion solution, we found it to be a good comprimise between power reduction and not reducing the performance too much.
The latter one being rather important, since noone wants to save 10% of energy, if they have to wait twice as long for the results.

## How to build

`$ make`

## How to run

`$ ./cc-energy-manager`

Possible options:

- `-config <configfile>`: Path to main configuration file
- `-debug`: Activate debugging output
- `-log <logfile>`: Write output to logfile instead of stdout

You can find a sample configuration file in [configs](configs/config.json).
Unfortunately it is not trivial to set it up.
We hopefully will add a more detailed description soon.
