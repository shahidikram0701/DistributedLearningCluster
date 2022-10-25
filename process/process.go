package process

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"cs425/mp/config"
	ml "cs425/mp/membershiplist"
	pb "cs425/mp/proto/coordinator_proto"
	cs "cs425/mp/proto/coordinator_sdfs_proto"
	dn "cs425/mp/proto/data_node_proto"
	intro_proto "cs425/mp/proto/introducer_proto"
	topology "cs425/mp/topology"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	network_topology *topology.Topology
	memberList       *ml.MembershipList
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
func GetAllCoordinators() []string {
	return memberList.GetAllCoordinators()
}

/**
* Bootstrapping the process
 */
func Run(port int, udpserverport int, log_process_port int, coordinatorServiceForLogsPort int, coordinatorServiceForSDFSPort int, datanodeServiceForSDFSPort int, wg *sync.WaitGroup, introAddr string, devmode bool, outboundIp net.IP) {
	// Start the coordinator server
	go StartCoordinatorService(coordinatorServiceForLogsPort, coordinatorServiceForSDFSPort, devmode, wg)

	go StartDataNodeService_SDFS(datanodeServiceForSDFSPort, wg)

	// Start the logger server
	go StartLogServer(log_process_port, wg)

	// Join the network. Get membership list from Introducer and update topology
	go JoinNetwork(introAddr, port, udpserverport, outboundIp, wg)

	// Start the UDP server to listen to pings and pongs
	go StartUdpServer(GetMemberList, udpserverport, wg)
}

func SendLogQueryRequest(coordinatorServiceForLogsPort int, query string) {
	coordinatorIp := memberList.GetCoordinatorNode()
	if coordinatorIp == "" {
		log.Printf("No master Node\n")
		return
	}
	// start a clock to time the execution time of the querying
	start := time.Now()
	coordinatorIp = fmt.Sprintf("%s:%d", coordinatorIp, coordinatorServiceForLogsPort)
	conn, err := grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	}

	defer conn.Close()

	// Initialise a client to connect to the coordinator process
	c := pb.NewCoordinatorServiceForLogsClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call the RPC function on the coordinator process to process the query
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: query, IsTest: false})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	} else {
		// mark the current time as the end time since the processing began
		duration := time.Since(start)

		// log the result and execution time
		log.Printf("Successfully fetched logs")
		fmt.Printf(r.GetLogs())
		log.Printf("Total Matches: %v", r.GetTotalMatches())
		log.Printf("\nExecution duration: %v", duration)

	}
}

/**
* Issue a request to the introducer to join the network
 */
func JoinNetwork(introducerAddress string, newProcessPort int, udpserverport int, outboundIp net.IP, wg *sync.WaitGroup) {
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

func getClientForCoordinatorService() (cs.CoordinatorServiceForSDFSClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")
	coordinatorAddr := fmt.Sprintf("%v:%v", memberList.GetCoordinatorNode(), conf.CoordinatorServiceSDFSPort)

	log.Printf("[ Client ]Coordinator is at: %v", coordinatorAddr)
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator: %v", err)
	}

	// defer conn.Close()

	// Initialise a client to connect to the coordinator process
	c := cs.NewCoordinatorServiceForSDFSClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	return c, ctx, conn, cancel
}

func PutFile(filename string) bool {
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()

	log.Printf("[ Client ]PutFile(%v): Intiating request to the coordinator!", filename)

	// Call the RPC function on the coordinator process to process the query
	r, err := client.PutFile(ctx, &cs.CoordinatorPutFileRequest{Filename: filename})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator: %v", err)
	} else {
		dataNodesForCurrentPut := r.DataNodes
		currentCommittedVersion := r.Version
		sequenceNumberOfThisPut := r.SequenceNumber

		// Stream the file to one of the data nodes (primary replica)
		// wait for a quorum to ack and then ask the server to bump the version of the file

		log.Printf("[ Client ]Allocated Nodes for the current PUT: %v", dataNodesForCurrentPut)
		log.Printf("[ Client ]Current Committed Version of the file: %v is %v", filename, currentCommittedVersion)
		log.Printf("[ Client ]Sequence number of the PUT: %v", sequenceNumberOfThisPut)

		status, err := saveFileToSDFS(filename, currentCommittedVersion, sequenceNumberOfThisPut, dataNodesForCurrentPut)

		if err != nil {
			log.Fatalf("[ Client ][ PutFile ]Cannot receive response: %v", err)
		}

		if status {
			log.Printf("[ Client ][ PutFile ]Successfully streamed the file %v to SDFS", filename)
		} else {
			log.Fatalf("[ Client ][ PutFile ]Failed for file %v", filename)
		}

		return true
	}
	return false
}

func saveFileToSDFS(filename string, currentCommittedVersion int64, sequenceNumberOfThisPut int64, dataNodesForCurrentPut []string) (bool, error) {
	conf := config.GetConfig("../../config/config.json")

	client, ctx, conn, cancel := getClientToPrimaryReplicaServer(dataNodesForCurrentPut[0]) // currently always picking the first allocated node as the primary replica
	defer conn.Close()
	defer cancel()

	stream, streamErr := client.DataNode_PutFile(ctx)
	if streamErr != nil {
		log.Printf("Cannot upload File: %v", streamErr)
	}

	filePath := fmt.Sprintf("%v/%v", conf.DataRootFolder, filename)

	log.Printf("[ Client ]Sending the file: %v", filePath)

	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("cannot open File: %v - %v", filePath, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := make([]byte, conf.ChunkSize)
	chunkId := 0

	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("cannot read chunk to buffer: %v", err)
		}
		req := &dn.Chunk{
			ChunkId:        int64(chunkId),
			Filename:       filename,
			Version:        currentCommittedVersion,
			Filesize:       100, // placeholder
			Chunk:          buffer[:n],
			SequenceNumber: sequenceNumberOfThisPut,
			ReplicaNodes:   dataNodesForCurrentPut[1:],
		}
		log.Printf("[ Client ][ PutFile ]Sending chunk %v of file: %v", chunkId, filename)
		e := stream.Send(req)
		if e != nil {
			log.Fatalf("[ Client ][ PutFile ]Cannot send chunk %v of file %v to server: %v", chunkId, filename, e)
		}
		chunkId++
	}

	res, err := stream.CloseAndRecv()

	if res.Status {
		log.Printf("[ Client ][ PutFile ]Sending commit message for File %v and Informing master of Version Bump", filename)
		sendCommitAndInformMasterOfUpdatedVersion(filename, currentCommittedVersion, sequenceNumberOfThisPut, dataNodesForCurrentPut[0]) // Currently Primary Replica is first of the allocated nodes
	} else {
		log.Fatalf("[ Client ][ PutFile ]Saving the file in SDFS failed")
	}

	return res.Status, err
}

func getClientToPrimaryReplicaServer(primaryReplicaIp string) (dn.DataNodeServiceClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")

	dataNodePort := conf.DataNodeServiceSDFSPort
	// primaryReplicaIp := dataNodesForCurrentPut[0] // currently always picking the first allocated node as the primary replica
	primaryReplica := fmt.Sprintf("%v:%v", primaryReplicaIp, dataNodePort)
	log.Printf("[ Client ]Primary Replica: %v", primaryReplica)
	conn, err := grpc.Dial(primaryReplica, grpc.WithInsecure())
	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	}

	// defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	// Initialise a client to connect to the coordinator process
	client := dn.NewDataNodeServiceClient(conn)

	return client, ctx, conn, cancel
}

func sendCommitAndInformMasterOfUpdatedVersion(filename string, currentCommittedVersion int64, sequenceNumberOfThisPut int64, primaryReplicaIp string) {
	// send commit to the data nodes
	client, ctx, conn, cancel := getClientToPrimaryReplicaServer(primaryReplicaIp) // currently always picking the first allocated node as the primary replica

	defer conn.Close()
	defer cancel()

	r, err := client.DataNode_CommitFile(ctx, &dn.DataNode_CommitFileRequest{Filename: filename, SequenceNumber: sequenceNumberOfThisPut})

	if err != nil {
		log.Fatalf("[ Client ][ PutFile ]Commit Failed")
	} else {
		status := r.GetStatus()
		newVersion := r.GetVersion()

		if status == false {
			log.Fatalf("[ Client ][ PutFile ]Commit Failed after getting response")
		} else {
			// Tell the master about updated version
			log.Printf("[ Client ][ PutFile ]New version after PutFile(%v) committed: %v", filename, newVersion)

			client, ctx, conn, cancel := getClientForCoordinatorService()

			defer conn.Close()
			defer cancel()

			// Call the RPC function on the coordinator process to process the query
			r, err := client.UpdateFileVersion(ctx, &cs.CoordinatorUpdateFileVersionRequest{
				Filename:       filename,
				SequenceNumber: sequenceNumberOfThisPut,
				Version:        currentCommittedVersion,
			})

			if err != nil {
				log.Fatalf("Failed to establish connection with the coordinator")
			} else {
				if r.Status {
					log.Printf("[ Client ]Successfully bumped the current committed version of the file on the coordinator")
				}
			}
		}
	}
}
