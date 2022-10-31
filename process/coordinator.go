package process

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"cs425/mp/config"
	pb "cs425/mp/proto/coordinator_proto"
	cs "cs425/mp/proto/coordinator_sdfs_proto"
	dn "cs425/mp/proto/data_node_proto"
	lg "cs425/mp/proto/logger_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CoordinatorState struct {
	lock                 *sync.RWMutex
	globalSequenceNumber map[string]int
	fileToNodeMapping    map[string][]string
	fileToVersionMapping map[string]int
	indexIntoMemberList  int
	myIpAddr             string // unchangable field - no lock reqd.
}

// list of machines acting as workers
var serverAddresses []string

var (
	coordinatorState *CoordinatorState
)

type CoordinatorServerForLogs struct {
	pb.UnimplementedCoordinatorServiceForLogsServer
}

type CoordinatorServerForSDFS struct {
	cs.UnimplementedCoordinatorServiceForSDFSServer
}

/**
* Adds an address to the list of all machine addresses
*
* @param addr: IP address of the machine
 */
func addServerAddress(addr string) {
	serverAddresses = append(serverAddresses, addr)
}

/**
* Send the query to the service process
*
* @param addr: IP address of the worker/service process
* @param query: query string
* @param isTest: boolean indicating if the function is triggered by a test client
* @param reponseChannel: channel for a connection between coordinator process and service process
 */
func queryServer(addr string, query string, isTest bool, responseChannel chan *lg.FindLogsReply) {
	tag := ""
	if isTest {
		tag = "[ TEST ]"
	}
	// Establish a TCP connection with a service process
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// RPC call to the service process to fetch logs
	r, err := c.FindLogs(ctx, &lg.FindLogsRequest{Query: query, IsTest: isTest})
	if err != nil {
		// may be service process is down
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	responseChannel <- r
}

/**
* The RPC function for querying logs
*
* @param ctx: context
* @param in: the query request
 */

func (s *CoordinatorServerForLogs) QueryLogs(ctx context.Context, in *pb.QueryRequest) (*pb.QueryReply, error) {
	query := in.GetQuery()
	isTest := in.GetIsTest()
	tag := ""

	conf := config.GetConfig("../../config/config.json")
	ml := GetMemberList()

	if isTest {
		tag = "[ TEST ]"
	}
	grepCommand := fmt.Sprintf("[ Coordinator ]grep -HEc '%v'", query)

	log.Printf("%vExecuting: %v", tag, grepCommand)

	// Establish connections with the server nodes
	responseChannel := make(chan *lg.FindLogsReply)
	numItems := 0

	// Concurrently establishing connections to all the service processes
	for memberListItem := range ml.Iter() {
		addr := (strings.Split(memberListItem.Id, ":"))[0] + fmt.Sprintf(":%d", conf.LoggerPort)
		numItems += 1
		go queryServer(addr, query, isTest, responseChannel)
	}
	logs := ""
	totalMatches := 0

	// Wait for all the service process to return the responses
	// Aggregate all the responses from service processes and redirect to the client
	for i := 0; i < numItems; i++ {
		logQueryResponse := <-responseChannel
		logs += logQueryResponse.GetLogs()
		totalMatches += int(logQueryResponse.GetNumMatches())
	}
	return &pb.QueryReply{Logs: logs, TotalMatches: int64(totalMatches)}, nil
}

/**
* Send a command to the service processes on the workers to generate logs
*
* @param addr: IP address of the worker/service process
* @param reponseChannel: channel for a connection between coordinator process and service process
* @param filenumer: file number of the test file to be generated
 */
func generateLogsOnServer(addr string, responseChannel chan *lg.GenerateLogsReply, filenumber int) {
	// Establish TCP connection with the service process
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Could not connect to node: %v", addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// RPC call to the service process to generate logs
	r, err := c.Test_GenerateLogs(ctx, &lg.GenerateLogsRequest{Filenumber: int32(filenumber)})
	if err != nil {
		log.Printf("Failed to generate Logs in: %v", addr)
	}
	responseChannel <- r
}

/**
* The RPC function for generating test logs on the service nodes
*
* @param ctx: context
* @param in: the query request
 */
func (s *CoordinatorServerForLogs) Test_GenerateLogs(ctx context.Context, in *pb.Test_Coordinator_GenerateLogsRequest) (*pb.Test_Coordinator_GenerateLogsReply, error) {
	// Establish connections with the server nodes
	responseChannel := make(chan *lg.GenerateLogsReply)

	// Concurrently establishing connections with service processes
	for idx, addr := range serverAddresses {
		go generateLogsOnServer(addr, responseChannel, idx+1)
	}
	status := ""

	// Wait for all the service processes to respond before aggregating response
	// and sending it to the client
	for _, addr := range serverAddresses {
		generateLogsResponse := <-responseChannel
		status += addr + ":" + generateLogsResponse.GetStatus()
	}
	return &pb.Test_Coordinator_GenerateLogsReply{Status: status}, nil
}

func StartCoordinatorService(coordinatorServiceForLogsPort int, coordinatorServiceForSDFSPort int, devmode bool, wg *sync.WaitGroup) {
	getOutboundIP := func() net.IP {
		conn, err := net.Dial("udp", "8.8.8.8:80")
		if err != nil {
			log.Printf("Couldn't get the IP address of the process\n%v", err)
		}
		defer conn.Close()

		localAddr := conn.LocalAddr().(*net.UDPAddr)

		return localAddr.IP
	}
	myIpAddr := getOutboundIP()
	// Initialise the state of the coordinator process
	coordinatorState = &CoordinatorState{
		lock:                 &sync.RWMutex{},
		globalSequenceNumber: make(map[string]int),
		fileToNodeMapping:    make(map[string][]string),
		fileToVersionMapping: make(map[string]int),
		myIpAddr:             fmt.Sprintf("%v", myIpAddr),
	}

	go coordintorService_ProcessLogs(coordinatorServiceForLogsPort, wg)
	go coordinatorService_SDFS(coordinatorServiceForSDFSPort, wg)
	go CoordinatorService_ReplicaRecovery(wg)
}

func coordintorService_ProcessLogs(port int, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServiceForLogsServer(s, &CoordinatorServerForLogs{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}

func (state *CoordinatorState) GetGlobalSequenceNumber(filename string) int {
	state.lock.Lock()
	defer state.lock.Unlock()

	_, ok := state.globalSequenceNumber[filename]

	if !ok {
		state.globalSequenceNumber[filename] = 0
	}

	currentGlobalSequenceNumber := state.globalSequenceNumber[filename]
	state.globalSequenceNumber[filename]++

	return currentGlobalSequenceNumber
}

func (state *CoordinatorState) PeekGlobalSequenceNumber(filename string) int {
	state.lock.RLock()
	defer state.lock.RUnlock()

	sequenceNum, ok := state.globalSequenceNumber[filename]

	if ok {
		return sequenceNum
	}
	return -1
}

func (state *CoordinatorState) GetNodeMappingsForFile(filename string) []string {
	state.lock.RLock()
	defer state.lock.RUnlock()

	return state.fileToNodeMapping[filename]
}

func (state *CoordinatorState) GetFileToNodeMappings() map[string][]string {
	state.lock.RLock()
	defer state.lock.RUnlock()

	return state.fileToNodeMapping
}

func (state *CoordinatorState) FileExists(filename string) bool {
	state.lock.RLock()
	defer state.lock.RUnlock()

	_, ok := state.fileToVersionMapping[filename]

	return ok
}

func (state *CoordinatorState) GenerateNodeMappingsForFile(filename string, numDataNodes int) []string {
	state.lock.Lock()
	defer state.lock.Unlock()

	nodes, newIndexIntoMemberlist := memberList.GetNDataNodes(state.indexIntoMemberList, numDataNodes)
	state.indexIntoMemberList = newIndexIntoMemberlist

	state.fileToNodeMapping[filename] = nodes

	log.Printf("[Coordinator]PutFile: Allocated nodes: %v", nodes)

	return nodes
}

func (state *CoordinatorState) UpdateNodeForFileAtIndex(filename string, idx int, newNode string) bool {
	state.lock.Lock()
	defer state.lock.Unlock()

	_, ok := state.fileToNodeMapping[filename]
	if ok {
		state.fileToNodeMapping[filename][idx] = newNode
	}

	return ok

}

func (state *CoordinatorState) GetVersionOfFile(filename string) int {
	state.lock.RLock()
	defer state.lock.RUnlock()

	if version, ok := state.fileToVersionMapping[filename]; ok {
		return version
	}
	return 0 // in the case it is a create operation
}

func (state *CoordinatorState) UpdateVersionOfFile(filename string) int {
	state.lock.Lock()
	defer state.lock.Unlock()

	_, ok := state.fileToVersionMapping[filename]

	newVersion := 1 // if need to update version after creating
	if ok {
		newVersion = state.fileToVersionMapping[filename] + 1
	}
	state.fileToVersionMapping[filename] = newVersion

	return newVersion
}

func (s *CoordinatorServerForSDFS) PutFile(ctx context.Context, in *cs.CoordinatorPutFileRequest) (*cs.CoordinatorPutFileReply, error) {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()
	log.Printf("[ Coordinator ][ PutFile ]PutFile(%v)", filename)
	operation := "Create"
	if coordinatorState.FileExists(filename) {
		operation = "Update"
	}

	log.Printf("[ Coordinator ][ PutFile ] operation: %v", operation)

	// get a sequence number for the current operation and
	// increment the global sequence number
	sequenceNumber := coordinatorState.GetGlobalSequenceNumber(filename)
	log.Printf("[ Coordinator ][ PutFile ]Sequence number for File: %v is %v", filename, sequenceNumber)

	var nodeMappings []string
	if operation == "Update" {
		nodeMappings = coordinatorState.GetNodeMappingsForFile(filename)
	} else {
		// allocate nodes for this file and update the file mapping
		log.Printf("[ Coordinator ][ PutFile ]Generating blocks for the file")
		nodeMappings = coordinatorState.GenerateNodeMappingsForFile(filename, conf.NumOfReplicas)
	}

	log.Printf("[ Coordinator ][ PutFile ]%v operation sequenced at %v on the file: %v; Data nodes: %v;\n", operation, sequenceNumber, filename, nodeMappings)

	return &cs.CoordinatorPutFileReply{
		SequenceNumber: int64(sequenceNumber),
		Version:        int64(coordinatorState.GetVersionOfFile(filename)),
		DataNodes:      nodeMappings,
	}, nil
}

func (s *CoordinatorServerForSDFS) GetFile(ctx context.Context, in *cs.CoordinatorGetFileRequest) (*cs.CoordinatorGetFileReply, error) {
	filename := in.GetFilename()
	log.Printf("[ Coordinator ][ GetFile ]GetFile(%v)", filename)

	if !coordinatorState.FileExists(filename) {
		log.Printf("[ Coordinator ][ GetFile ]File %v doesn't exist", filename)
		return nil, errors.New("File doesn't exist")
	}

	sequenceNumber := coordinatorState.GetGlobalSequenceNumber(filename)
	log.Printf("[ Coordinator ][ GetFile ]Sequence number for File: %v is %v", filename, sequenceNumber)

	nodeMappings := coordinatorState.GetNodeMappingsForFile(filename)

	log.Printf("[ Coordinator ][ GetFile ]Operation sequenced at %v on the file: %v; Data nodes: %v;\n", sequenceNumber, filename, nodeMappings)

	return &cs.CoordinatorGetFileReply{
		SequenceNumber: int64(sequenceNumber),
		Version:        int64(coordinatorState.GetVersionOfFile(filename)),
		DataNodes:      nodeMappings,
	}, nil
}

func (s *CoordinatorServerForSDFS) UpdateFileVersion(ctx context.Context, in *cs.CoordinatorUpdateFileVersionRequest) (*cs.CoordinatorUpdateFileVersionReply, error) {
	filename := in.Filename
	log.Printf("[ Coordinator ][ PutFile ]Updating the version number of the file: %v", filename)

	coordinatorState.UpdateVersionOfFile(filename)

	return &cs.CoordinatorUpdateFileVersionReply{
		Status: true,
	}, nil
}

func coordinatorService_SDFS(port int, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	cs.RegisterCoordinatorServiceForSDFSServer(s, &CoordinatorServerForSDFS{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}

func replicaRepair() {
	fileMapping := coordinatorState.GetFileToNodeMappings()

	for filename, nodes := range fileMapping {
		for idx, node := range nodes {
			if !memberList.IsNodeAlive(node) {
				log.Printf("[ Coordinator ][ Replica Recovery ]Replica %v for file: %v is down", node, filename)
				go assignNewReplicaAndReplicate(filename, nodes, node, idx)
			}
		}
	}
}

func assignNewReplicaAndReplicate(filename string, nodes []string, nodeToRecover string, nodeIdx int) {
	contains := func(s []string, str string) bool {
		for _, v := range s {
			if v == str {
				return true
			}
		}

		return false
	}
	newNode := memberList.GetRandomNode()
	for {
		if !contains(nodes, newNode) {
			break
		}
		newNode = memberList.GetRandomNode()
	}

	log.Printf("[ Coordinator ][ Replica Recovery ]Node %v to selected to replace the down node %v", newNode, nodeToRecover)

	activeReplica := ""

	// Select the node in the replica set that should help the new node come to speed
	for idx, n := range nodes {
		if idx == nodeIdx {
			continue
		}
		if memberList.IsNodeAlive(n) {
			activeReplica = n
			break
		}
	}

	log.Printf("[ Coordinator ][ Replica Recovery ]Replica %v is up and is assigned to handle failure of node %v", activeReplica, nodeToRecover)

	client, ctx, conn, cancel := getClientToReplicaServer(newNode)
	defer conn.Close()
	defer cancel()

	res, err := client.DataNode_InitiateReplicaRecovery(ctx, &dn.DataNode_InitiateReplicaRecoveryRequest{Filename: filename, NodeToReplicateDataFrom: activeReplica})

	if err != nil {
		log.Printf("[ Coordinator ][ Replica Recovery ]Replica Recovery of node %v failed - %v", nodeToRecover, err)
	} else {
		if res.Status {
			coordinatorState.UpdateNodeForFileAtIndex(filename, nodeIdx, newNode)

			log.Printf("[ Coordinator ][ Replica Recovery ]Replica Recovery of node %v successful; Replaced with the node %v", nodeToRecover, newNode)
		}
	}
}

func CoordinatorService_ReplicaRecovery(wg *sync.WaitGroup) {
	conf := config.GetConfig("../../config/config.json")
	ticker := time.NewTicker(time.Duration(conf.ReplicaRecoveryInterval) * time.Second)
	quit := make(chan struct{})
	func() {
		for {
			select {
			case <-ticker.C:
				currentCoordinator := memberList.GetCoordinatorNode()
				myIpAddr := coordinatorState.myIpAddr

				if currentCoordinator == myIpAddr {
					// fmt.Printf("[ Coordinator ][ Replica Recovery ]")
					log.Printf("[ Coordinator ][ Replica Recovery ]Initialising\n")
					replicaRepair()
				}
				// close(quit)
			case <-quit:
				if memberList.GetCoordinatorNode() == coordinatorState.myIpAddr {
					log.Printf("[ Coordinator ][ Replica Recovery ]Termination")
					ticker.Stop()
				}
				wg.Done()
				return
			}
		}
	}()
}
