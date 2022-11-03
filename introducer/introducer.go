package introducer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"cs425/mp/config"
	ml "cs425/mp/membershiplist"
	"cs425/mp/process"
	intro "cs425/mp/proto/introducer_proto"
	"cs425/mp/topology"

	"google.golang.org/grpc"
)

type server struct {
	intro.UnimplementedIntroducerServer
}

var (
	network_topology *topology.Topology
	memberList       *ml.MembershipList = ml.NewMembershipList()
	coordinatorList  map[string]bool
)

/**
* Get the Membership list of this process
 */
func GetMemberList() *ml.MembershipList {
	return memberList
}

/**
* Get the Network topology of this process
 */
func GetNetworkTopology() *topology.Topology {
	return network_topology
}

/**
* Bootstrap the introducer
 */
func Run(devmode bool, port int, udpserverport int, wg *sync.WaitGroup) {
	// Start the introducer and listen to TCP connections on one thread
	go StartIntroducerAndListenToConnections(devmode, port, udpserverport, wg)

	// Start the UDP server on another thread
	go process.StartUdpServer(GetMemberList, udpserverport, wg)
}

/**
* Start the TCP server for the introducer to listen to join requests from
* processes
 */
func StartIntroducerAndListenToConnections(devmode bool, port int, udpserverport int, wg *sync.WaitGroup) {
	coordinatorList = make(map[string]bool)
	introducerAddress := "172.22.156.122"

	if devmode {
		introducerAddress = "192.168.64.3"
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

	id := fmt.Sprintf("%s:%d:%v", introducerAddress, port, time.Now().Nanosecond())
	// Adding itself(introducer) to the membership list
	memberList.Append(ml.MembershipListItem{
		Id:                id,
		State:             ml.NodeState{Timestamp: time.Now(), Status: ml.Alive},
		IncarnationNumber: 0,
		UDPPort:           udpserverport,
		IsCoordinator:     false,
		IsIntroducer:      true,
	})

	log.Printf("Initialising Topology")
	network_topology = topology.InitialiseTopology(id, 0, udpserverport)

	log.Printf("Starting the topology stabilisation")
	go network_topology.StabiliseTheTopology(wg, memberList)

	// Ping neighbours for failure detection
	process.SendPings(wg, network_topology, GetMemberList())

	if err != nil {
		log.Printf("failed to listen: %v", err)
	}

	// Create, register and start the TCP server to listen to gRPC calls from processes that
	// wants to join the node
	s := grpc.NewServer()
	intro.RegisterIntroducerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		wg.Done()
	}
}

/**
* Start the TCP server for the introducer to listen to join requests from
* processes
 */
func (s *server) Introduce(ctx context.Context, in *intro.IntroduceRequest) (*intro.IntroduceReply, error) {
	if _, err := os.Stat("../../config/config.json"); os.IsNotExist(err) {
		log.Panicf("No config file found: %v\n", err)
	}
	conf := config.GetConfig("../../config/config.json")

	requestorIP := in.Ip
	requestorPort := in.Port
	requestorTimestamp := in.Timestamp
	udpserverport := in.Udpserverport

	// create an id for the new process
	newProcessId := fmt.Sprintf("%s:%d:%s", requestorIP, requestorPort, requestorTimestamp)

	log.Printf("Introducing process %s to the system", newProcessId)

	ipAndPort := fmt.Sprintf("%s:%d", requestorIP, requestorPort)
	isCoordinator := false
	if len(coordinatorList) < conf.NumOfCoordinators {
		isCoordinator = true
		coordinatorList[ipAndPort] = true
	} else {
		if _, ok := coordinatorList[ipAndPort]; ok {
			isCoordinator = true
		}
	}

	// Adding the new process to Introducer's membership list
	index := memberList.Append(ml.MembershipListItem{
		Id:                newProcessId,
		State:             ml.NodeState{Status: ml.Alive, Timestamp: time.Now()},
		IncarnationNumber: 0,
		UDPPort:           int(udpserverport),
		IsCoordinator:     isCoordinator,
		IsIntroducer:      false,
	})

	// Introducer needs to send the complete membership list to the new node
	reply := intro.IntroduceReply{}
	if serialisedMemberList, err := json.Marshal(memberList.GetList()); err == nil {
		reply.MembershipList = serialisedMemberList
		reply.Index = int64(index)
		reply.ProcessId = newProcessId
	} else {
		log.Printf("Error Marshalling the membership list to be sent\n%v", err)
	}

	return &reply, nil
}
