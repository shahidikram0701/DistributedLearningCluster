package process

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"cs425/mp/config"
	ml "cs425/mp/membershiplist"
	intro_proto "cs425/mp/proto/introducer_proto"
	topology "cs425/mp/topology"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	network_topology *topology.Topology
	memberList       *ml.MembershipList
	isCoordinator    bool
)

var (
	T_PING_SECOND = 0.5 // second
)

/**
* Get the membership list at this process
 */
func GetMemberList() *ml.MembershipList {
	return memberList
}

/**
* Get the network topology of this process
 */
func GetNetworkTopology() *topology.Topology {
	return network_topology
}

/**
* Check if the current process is a designated coordinator process
 */
func IsCoordinator() bool {
	return isCoordinator
}

/**
* Bootstrapping the process
 */
func Run(port int, udpserverport int, log_process_port int, coordinator_process_port int, wg *sync.WaitGroup, introAddr string, devmode bool, outboundIp net.IP) {
	// Start the coordinator server
	go StartCoordinatorService(coordinator_process_port, devmode, wg)

	// Start the logger server
	go StartLogServer(log_process_port, wg)

	// Join the network. Get membership list from Introducer and update topology
	go JoinNetwork(introAddr, port, udpserverport, outboundIp, wg)

	// Start the UDP server to listen to pings and pongs
	go StartUdpServer(GetMemberList, udpserverport, wg)
}

/**
* Issue a request to the introducer to join the network
 */
func JoinNetwork(introducerAddress string, newProcessPort int, udpserverport int, outboundIp net.IP, wg *sync.WaitGroup) {
	conf := config.GetConfig("../../config/config.json")
	var conn *grpc.ClientConn
	var err error
	log.Printf("Establishing connection to introducer process at %v", introducerAddress)
	conn, err = grpc.Dial(introducerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to establish connection with the introducer\n%v", err)
	}

	defer conn.Close()

	// Initialise a client to connect to the introducer process
	c := intro_proto.NewIntroducerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call the RPC function on the introducer process to join the network
	r, err := c.Introduce(ctx, &intro_proto.IntroduceRequest{
		Ip:            fmt.Sprintf("%v", outboundIp),
		Port:          int64(newProcessPort),
		Timestamp:     fmt.Sprintf("%d", time.Now().Nanosecond()),
		Udpserverport: int64(udpserverport),
	})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the introducer\n%v", err)
	} else {
		var membershipList []ml.MembershipListItem
		unmarshallingError := json.Unmarshal(r.MembershipList, &membershipList)
		if unmarshallingError != nil {
			log.Printf("Error while unmarshalling the membershipList: %v\n", unmarshallingError)
		}

		log.Printf("Received Membership List from the introducer\n")
		if len(membershipList) <= conf.NumOfCoordinators+1 {
			isCoordinator = true
		}
		memberList = ml.NewMembershipList(membershipList)
		myIndex := int(r.Index)
		myId := r.ProcessId
		log.Printf("Intialised self memberList\n")

		network_topology = topology.InitialiseTopology(myId, myIndex, udpserverport)
		log.Printf("Initialised Topology: %v\n", network_topology)
		var exited = make(chan bool)

		log.Printf("Starting the topology stabilisation\n")
		go network_topology.StabiliseTheTopology(wg, memberList)

		log.Printf("Starting the SWIM ping-pongs\n")
		go SendPings(wg, network_topology, memberList)

		<-exited

	}
	wg.Done()
}

// Pings the neighbours every T_PING_SECOND for failure detection
func SendPings(wg *sync.WaitGroup, network_topology *topology.Topology, memberList *ml.MembershipList) {
	ticker := time.NewTicker(time.Duration(T_PING_SECOND*1000) * time.Millisecond)
	quit := make(chan struct{})

	go func() {
		getNodeToPing := getWhichNeighbourToPing(network_topology)
		for {
			select {
			case <-ticker.C:
				SendPing(getNodeToPing, network_topology, memberList)
			case <-quit:
				ticker.Stop()
				wg.Done()
				return
			}
		}
	}()
}

/**
* Get which of the predecessor, successor or super successor
* is to be pinged in the current protocol period
 */
func getWhichNeighbourToPing(network_topology *topology.Topology) func() topology.Node {
	i := -1

	return func() topology.Node {
		// Minimum number of nodes needed for neighbours = 3
		// If the number of processes joined so far is less than 3
		// consider that for looping over the neighbours
		numberOfNodesInTopology := network_topology.GetNumberOfNodes()
		i = (i + 1) % int(math.Min(3, float64(numberOfNodesInTopology)))

		switch i {
		case 0:
			predecessor := network_topology.GetPredecessor()
			log.Printf("Piging predecessor: %v\n", predecessor)
			return predecessor
		case 1:
			successor := network_topology.GetSuccessor()
			log.Printf("Piging successor: %v\n", successor)
			return successor
		case 2:
			supersuccessor := network_topology.GetSuperSuccessor()
			log.Printf("Piging supersuccessor: %v\n", supersuccessor)
			return supersuccessor
		}

		return network_topology.GetPredecessor()
	}
}

/**
* Called when a process wants to leave the cluster. A delay is given to ensure
* membership list propagation to all nodes
 */

func LeaveNetwork() {
	log.Printf("Leaving Network\n")
	me := network_topology.GetSelfNodeId()
	memberList.MarkLeave(me)
	time.Sleep(3 * time.Second)
	os.Exit(3)
}
