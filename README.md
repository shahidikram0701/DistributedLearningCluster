# [Group 37] Distributed Log Querier

This project is an implementation of a basic distributed log querier. 

## Description

We have implemented a distributed system where a client process can query for logs from a set of machines. The client is agnostic to the number of machines and the specifics of the distributed environment.

The client process first chooses a coordinator process to which the query request is to be forwarded to. If the connection to a chosen coordinator fails, the client randomly chooses another coordinator process until a connection succeeds. This ensures that even if there is one coordinator process running the client's request will be processed.

The coordinator process then forwards the query request to the worker/service processes who process the query on the chunk of log file that resides on them and returns the result to the coordinator. The coordinator node assimilates the results from all the service processes and returns a response back to the client. 

## Directory structure

```
cs425_mp1_distributedlogquerier
│    │   README.md
│    │   run.sh
│    │   stop.sh
│    │   removelogs.sh
│    │   go.mod
│    │   go.sum
│    │   
│    └───client
│    │   │   client.go
│    │   
│    └───coordinator
│    │   │   coordinator.go
│    │   │   coordinator.log
│    │   
│    └───service
│    │   │   service.go
│    │   │   service.log 
│    │
│    └───proto
│    │   │
│    │   └───coordinator_proto
│    │   │   │   coordinator.proto
│    │   │   │   coordinator.pb.go
│    │   │   │   coordinator_grpc.pb.go
│    │   │
│    │   └───logger_proto
│    │       │   logger.proto
│    │       │   logger.pb.go
│    │       │   logger_grpc.pb.go
│    │  
│    └───testclient
│    │   │   testclient.go
│    │
│    └───test_log_scripts
│    │   │   log_gen1.sh
│    │   │   log_gen2.sh
│    │   │   ...
│    │   │   log_gen10.sh
│
logs
│    │   vm(i).log
```

## Running the code
```
Setting up the code

$ git clone https://gitlab.engr.illinois.edu/shahidi3/cs425_mp1_distributedlogquerier.git
$ mkdir logs (should contain log pertaining to the node)
$ cd cs425_mp1_distributedlogquerier
$ go mod download

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

## Sample output

![Sample Output](output.png?raw=true "Title")

## Authors

Lavanya Ramkumar\
Shahid Ikram