# [Group 37] Distributed System

This project is an implementation of a basic distributed log querier and a distributed Failure detector

## Description


### Distributed Log Querier
We have implemented a distributed system where a client process can query for logs from a set of machines. The client is agnostic to the number of machines and the specifics of the distributed environment.

The client process first chooses a coordinator process to which the query request is to be forwarded to. If the connection to a chosen coordinator fails, the client randomly chooses another coordinator process until a connection succeeds. This ensures that even if there is one coordinator process running the client's request will be processed.

The coordinator process then forwards the query request to the worker/service processes who process the query on the chunk of log file that resides on them and returns the result to the coordinator. The coordinator node assimilates the results from all the service processes and returns a response back to the client. 

### Distributed Group Membership
We have implemented a SWIM style distributed group membership protocol, where in each protocol period  (0.5s), each machine in the topology (ring shaped) pings one of the neighbors (predecessor, successor, super-successor) in a cyclic way, making sure it monitors all the 3 neighbors over three protocol periods. This design ensures that 3 simultaneous failures are detected. This design scales well for a large number of nodes because each process just monitors 3 of its neighbors and sends a copy of the membership list to them instead of flooding the whole topology with the list which could lead to increased network congestion as in all-to-all heartbeat failure detection. We also piggy back the marshaled membership list with every “pong” that the node sends out ensuring gossip style membership list dissemination. 


The state of each of the entries in the membership list goes from “Active” -> “Suspicious” -> “Failed” -> “Delete”, after which the entry for that process is deleted. If the process doesn’t respond to the UDP ping the process is marked “Suspicious” and if it remains suspicious for T_FAIL = 1s, the state is updated to “Failed”. Once the process is marked as failed, we wait for another T_DELETE = 1s to mark the process state as “Delete” after which the process will be removed from the list. This ensures that a failed process will be removed from the list in well under 3s. 


## Running the code

### Setting up the code
```
$ git clone https://gitlab.engr.illinois.edu/shahidi3/cs425_mp1_distributedlogquerier.git
$ mkdir logs (should contain log pertaining to the node)
$ cd cs425_mp1_distributedlogquerier
$ go mod download
```

### Running the Distributed Log Querier
```
[optional] To delete the previous logs of coordinator and the service

$ ./removelogs.sh 

To start the coordinator and the service on the node

$ ./run.sh

To run the client and query for an arbitrary query

$ cd client
$ go run client.go -query "searchQuery"

To run the test client

$ cd testclient
$ go run testclient

To stop the coordinator and service processes in the end

$ ./stop.sh
```

### Running the Distributed Group Membership
```
[optional] To delete the previous logs

$ ./removelogs.sh 

Starting the introducer

$ cd commands/introducer
$ go run introducer.go

Starting a process
$ go run commands/process
$ go run process.go

```

## Authors

Lavanya Ramkumar\
Shahid Ikram