package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	log.Printf("Getting the grpc cliend for the Coordinator at: %v", coordinatorAddr)
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

func PutFile(filename string, localfilename string) bool {
	conf := config.GetConfig("../../config/config.json")
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()
	retries := 0
	for {
		log.Printf("[ Client ]PutFile(%v): Intiating request to the coordinator!", filename)
		filePath := fmt.Sprintf("%v/%v", conf.DataRootFolder, localfilename)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Printf("[ Client ][ PutFile ]File %v doesnt exist", localfilename)
			return false
		}

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

			status, err := saveFileToSDFS(filename, localfilename, currentCommittedVersion, sequenceNumberOfThisPut, dataNodesForCurrentPut)

			if err != nil {
				log.Printf("[ Client ][ PutFile ]Cannot receive response: %v", err)
				log.Printf("[ Client ][ PutFile ]Retrying...")
			}

			if status {
				log.Printf("[ Client ][ PutFile ]Successfully streamed the file %v to SDFS", filename)
				break
			} else {
				log.Printf("[ Client ][ PutFile ]Failed for file %v", filename)
				log.Printf("[ Client ][ PutFile ]Retrying...")
			}
		}
		retries++
		if retries > conf.NumRetriesPerOperation {
			return false
		}
	}
	return true
}

func ListAllNodesForAFile(filename string) []string {
	client, ctx, conn, cancel := getClientForCoordinatorService()
	dataNodes := []string{}
	defer conn.Close()
	defer cancel()
	log.Printf("[ Client ][ ListNodes ]ls(%v): Intiating request to the coordinator!", filename)

	r, err := client.ListAllNodesForFile(ctx, &cs.CoordinatorListAllNodesForFileRequest{FileName: filename})

	if err != nil {
		log.Printf("[ Client ][ ListNodes ]Failed to establish connection with the coordinator: %v", err)
	} else {
		dataNodes = r.GetDataNodes()

		log.Printf("[ Client ][ ListNodes ]Allocated Nodes for the current file: %v", dataNodes)
	}

	return dataNodes
}
func GetFile(filename string, localfilename string) bool {
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()
	log.Printf("[ Client ][ GetFile ]GetFile(%v): Intiating request to the coordinator!", filename)

	// Call the RPC function on the coordinator process to process the query
	r, err := client.GetFile(ctx, &cs.CoordinatorGetFileRequest{Filename: filename})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("[ Client ][ GetFile ]Error: %v", err)
		return false
	} else {
		dataNodesForCurrentGet := r.DataNodes
		currentCommittedVersion := r.Version
		sequenceNumberOfThisGet := r.SequenceNumber

		log.Printf("[ Client ][ GetFile ]Replicas containing the file: %v", dataNodesForCurrentGet)
		log.Printf("[ Client ][ GetFile ]Current Committed Version of the file: %v is %v", filename, currentCommittedVersion)
		log.Printf("[ Client ][ GetFile ]Sequence number of the Get: %v", sequenceNumberOfThisGet)

		status, err := getFileFromSDFS(filename, localfilename, currentCommittedVersion, sequenceNumberOfThisGet, dataNodesForCurrentGet)
		log.Printf("[ Client ][ GetFile ]The execution of the GetFile operation of file %v with sequenceNumber %v done on all the data nodes %v", filename, sequenceNumberOfThisGet, dataNodesForCurrentGet)

		// log.Printf("[ Client ][ GetFile ]Updating the sequence number for getFile(%v) with sequence number %v on all the data nodes", filename, sequenceNumberOfThisGet)

		// update the sequence number for the file on all its data nodes
		// for _, dataNode := range dataNodesForCurrentGet {
		// 	go updateSequenceNumberForTheFile(filename, sequenceNumberOfThisGet, dataNode)
		// }

		if err != nil {
			log.Printf("[ Client ][ GetFile ]Error getting file %v:%v - %v", filename, currentCommittedVersion, err)
			return status
		}

		if !status {
			log.Printf("[ Client ][ GetFile ]Failed for file %v:%v - %v", filename, currentCommittedVersion, err)
			return status
		}
	}
	log.Printf("[ Client ][ GetFile ]Successfully fetched the file %v from SDFS", filename)
	return true
}

func GetFileVersions(filename string, numVersions int, localfilename string) bool {
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()
	log.Printf("[ Client ][ GetFileVersions ]GetFileVersions(%v, %v): Intiating request to the coordinator!", filename, numVersions)

	// Call the RPC function on the coordinator process to process the query
	r, err := client.GetFileVersions(ctx, &cs.CoordinatorGetFileVersionsRequest{Filename: filename})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("[ Client ][ GetFileVersions ]Error: %v", err)
		return false
	} else {
		dataNodesForCurrentGet := r.DataNodes
		currentCommittedVersion := r.Version

		log.Printf("[ Client ][ GetFileVersions ]Replicas containing the file: %v", dataNodesForCurrentGet)
		log.Printf("[ Client ][ GetFileVersions ]Current Committed Version of the file: %v is %v", filename, currentCommittedVersion)

		status, err := getFileVersionsFromSDFS(filename, localfilename, currentCommittedVersion, dataNodesForCurrentGet, int64(numVersions))
		log.Printf("[ Client ][ GetFileVersions ]The execution of the GetFileVersions operation of file %v done on all the data nodes %v", filename, dataNodesForCurrentGet)

		// log.Printf("[ Client ][ GetFileVersions ]Updating the sequence number for getFile(%v) with sequence number %v on all the data nodes", filename, sequenceNumberOfThisGet)

		// update the sequence number for the file on all its data nodes
		// for _, dataNode := range dataNodesForCurrentGet {
		// 	go updateSequenceNumberForTheFile(filename, sequenceNumberOfThisGet, dataNode)
		// }

		if err != nil {
			log.Printf("[ Client ][ GetFileVersions ]Error getting %v versions of the file %v:%v - %v", numVersions, filename, currentCommittedVersion, err)
			return status
		}

		if !status {
			log.Printf("[ Client ][ GetFileVersions ]Failed for file %v versions of the file %v:%v - %v", numVersions, filename, currentCommittedVersion, err)
			return status
		}
	}
	log.Printf("[ Client ][ GetFile ]Successfully fetched the file %v from SDFS", filename)
	return true
}

func updateSequenceNumberForTheFile(filename string, seqNum int64, dataNode string) {
	log.Printf("[ Client ][ GetFile ]Initiating update Sequence number on node %v for file %v. The new sequence number would be: %v", dataNode, filename, seqNum+1)

	client, ctx, conn, cancel := getClientToReplicaServer(dataNode)
	defer conn.Close()
	defer cancel()

	_, err := client.DataNode_UpdateSequenceNumber(ctx, &dn.DataNode_UpdateSequenceNumberRequest{
		Filename:       filename,
		SequenceNumber: seqNum + 1,
	})

	if err != nil {
		log.Printf("[ Client ][ ReadFile ]Update sequence number for file %v on node %v to %v FAILED. Error: %v", filename, dataNode, seqNum+1, err)
	} else {
		log.Printf("[ Client ][ ReadFile ]Successfully updated sequence number for file %v on node %v to %v", filename, dataNode, seqNum+1)
	}

}

func DeleteFile(filename string) bool {
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()
	log.Printf("[ Client ][ DeleteFile ]DeleteFile(%v): Intiating request to the coordinator!", filename)

	// Call the RPC function on the coordinator process to process the query
	r, err := client.DeleteFile(ctx, &cs.CoordinatorDeleteFileRequest{Filename: filename})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("[ Client ][ DeleteFile ]Error: %v", err)
		return false
	} else {
		replicas := r.GetReplicas()
		sequenceNumberOfThisDelete := r.GetSequenceNumber()

		log.Printf("[ Client ][ DeleteFile ]Replicas containing the file: %v", replicas)
		log.Printf("[ Client ][ DeleteFile ]Sequence number of the Delete: %v", sequenceNumberOfThisDelete)

		status, err := deleteFileInSDFS(filename, sequenceNumberOfThisDelete, replicas)

		if err != nil {
			log.Printf("[ Client ][ DeleteFile ]Error deleting file %v: %v", filename, err)
			return status
		}

		if !status {
			log.Printf("[ Client ][ DeleteFile ]Failed for file %v - %v", filename, err)
			return status
		}
		log.Printf("[ Client ][ DeleteFile ]Sending delete commit to all the data nodes %v containing file %v and informing master about the update", replicas, filename)
		sendDeleteCommitAndInformMaster(filename, sequenceNumberOfThisDelete, replicas)
	}

	log.Printf("[ Client ][ DeleteFile ]Successfully deleted the file %v from SDFS", filename)
	return true
}

func saveFileToSDFS(filename string, localfilename string, currentCommittedVersion int64, sequenceNumberOfThisPut int64, dataNodesForCurrentPut []string) (bool, error) {
	conf := config.GetConfig("../../config/config.json")

	client, ctx, conn, cancel := getClientToReplicaServer(dataNodesForCurrentPut[0]) // currently always picking the first allocated node as the primary replica
	defer conn.Close()
	defer cancel()

	stream, streamErr := client.DataNode_PutFile(ctx)
	if streamErr != nil {
		log.Printf("[ Client ][ PutFile ]Cannot upload File: %v", streamErr)
		return false, nil
	}

	filePath := fmt.Sprintf("%v/%v", conf.DataRootFolder, localfilename)

	log.Printf("[ Client ][ PutFile ]Sending the file: %v", filePath)

	file, err := os.Open(filePath)

	if err != nil {
		// fmt.Printf("File %v doesn't exist :(", filePath)
		log.Printf("[ Client ][ PutFile ]Cannot open File: %v, File doesnt exist - err: %v", filePath, err)
		return false, err
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
			log.Printf("[ Client ][ PutFile ]Cannot read chunk to buffer: %v", err)
			return false, err
		}
		req := &dn.Chunk{
			ChunkId:        int64(chunkId),
			Filename:       filename,
			Version:        currentCommittedVersion,
			Filesize:       100, // placeholder
			Chunk:          buffer[:n],
			SequenceNumber: sequenceNumberOfThisPut,
			ReplicaNodes:   dataNodesForCurrentPut[1:],
			IsReplicaChunk: false,
		}
		log.Printf("[ Client ][ PutFile ]Sending chunk %v of file: %v", chunkId, filename)
		e := stream.Send(req)
		if e != nil {
			log.Printf("[ Client ][ PutFile ]Cannot send chunk %v of file %v to server: %v", chunkId, filename, e)
			return false, e
		}
		chunkId++
	}

	res, err := stream.CloseAndRecv()

	if err != nil {
		log.Printf("[ Client ][ PutFile ]Putfile(%v) failed after streaming content; error: %v", filename, err)
		return false, err
	}

	if res.Status {
		log.Printf("[ Client ][ PutFile ]Sending commit message for File %v and Informing master of Version Bump", filename)
		sendCommitAndInformMasterOfUpdatedVersion(filename, currentCommittedVersion, sequenceNumberOfThisPut, dataNodesForCurrentPut) // Currently Primary Replica is first of the allocated nodes
	} else {
		log.Printf("[ Client ][ PutFile ]Saving the file in SDFS failed")
		return false, errors.New("Saving the file in SDFS failed")
	}

	return res.Status, err
}

func getFileFromSDFS(filename string, localfilename string, currentCommittedVersion int64, sequenceNumberOfThisGet int64, replicas []string) (bool, error) {
	conf := config.GetConfig("../../config/config.json")
	client, ctx, conn, cancel := getClientToReplicaServer(replicas[0])
	defer conn.Close()
	defer cancel()

	stream, err := client.DataNode_GetFile(ctx, &dn.DataNode_GetFileRequest{
		Filename:       filename,
		SequenceNumber: sequenceNumberOfThisGet,
		Replicas:       replicas[1:],
		Version:        currentCommittedVersion,
	})

	if err != nil {
		log.Printf("[ Client ][ GetFile ]Getting file %v:%v FAILED", filename, currentCommittedVersion)
		return false, err
	}

	data := bytes.Buffer{}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[ Client ][ GetFile ]Got all the chunks of the file %v-%v", filename, currentCommittedVersion)
			break
		}
		if err != nil {
			return false, err
		}
		_, err = data.Write(chunk.GetChunk())
		if err != nil {
			log.Panicf("[ Client ][ GetFile ]Chunk aggregation failed - %v", err)
			return false, err
		}
	}

	if _, err := os.Stat(conf.OutputDataFolder); os.IsNotExist(err) {
		err := os.Mkdir(conf.OutputDataFolder, os.ModePerm)
		if err != nil {
			log.Printf("[ Client ][ GetFile ]Error creating output folder\n")
			return false, err
		}
	}

	folder_for_the_file := fmt.Sprintf("%v/%v", conf.OutputDataFolder, localfilename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		err := os.Mkdir(folder_for_the_file, os.ModePerm)
		if err != nil {
			log.Printf("[ Client ][ GetFile ]Error creating folder %v\n", folder_for_the_file)

			return false, err
		}
	}

	err = os.WriteFile(fmt.Sprintf("%v/%v-%v", folder_for_the_file, localfilename, currentCommittedVersion), data.Bytes(), 0644)

	if err != nil {
		log.Printf("[ Client ][ GetFile ]File getting failed: %v", err)
		return false, err
	}

	return true, nil
}

func getFileVersionsFromSDFS(filename string, localfilename string, currentCommittedVersion int64, replicas []string, numVersions int64) (bool, error) {
	conf := config.GetConfig("../../config/config.json")
	client, ctx, conn, cancel := getClientToReplicaServer(replicas[0])
	defer conn.Close()
	defer cancel()

	stream, err := client.DataNode_GetFileVersions(ctx, &dn.DataNode_GetFileVersionsRequest{
		Filename:    filename,
		Replicas:    replicas[1:],
		Version:     currentCommittedVersion,
		NumVersions: numVersions,
	})

	if err != nil {
		log.Printf("[ Client ][ GetFileVersions ]Getting %v versions of the file %v:%v FAILED", numVersions, filename, currentCommittedVersion)
		return false, err
	}

	data := bytes.Buffer{}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[ Client ][ GetFileVersions ]Got all the chunks of the %v versions of the file %v-%v", numVersions, filename, currentCommittedVersion)
			break
		}
		if err != nil {
			return false, err
		}
		_, err = data.Write(chunk.GetChunk())
		if err != nil {
			log.Panicf("[ Client ][ GetFileVersions ]Chunk aggregation failed - %v", err)
			return false, err
		}
	}

	if _, err := os.Stat(conf.OutputDataFolder); os.IsNotExist(err) {
		err := os.Mkdir(conf.OutputDataFolder, os.ModePerm)
		if err != nil {
			log.Printf("[ Client ][ GetFileVersions ]Error creating output folder\n")
			return false, err
		}
	}

	folder_for_the_file := fmt.Sprintf("%v/%v", conf.OutputDataFolder, localfilename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		err := os.Mkdir(folder_for_the_file, os.ModePerm)
		if err != nil {
			log.Printf("[ Client ][ GetFileVersions ]Error creating folder %v\n", folder_for_the_file)

			return false, err
		}
	}

	err = os.WriteFile(fmt.Sprintf("%v/%v-latest_%v_versions", folder_for_the_file, localfilename, numVersions), data.Bytes(), 0644)

	if err != nil {
		log.Printf("[ Client ][ GetFileVersions ]File getting failed: %v", err)
		return false, err
	}

	return true, nil
}

func deleteFileInSDFS(filename string, sequenceNumberOfThisDelete int64, replicas []string) (bool, error) {
	client, ctx, conn, cancel := getClientToReplicaServer(replicas[0]) // currently always picking the first allocated node as the primary replica
	defer conn.Close()
	defer cancel()

	r, err := client.DataNode_DeleteFileQuorumCheck(ctx, &dn.DataNode_DeleteFileQuorumCheckRequest{
		Filename:       filename,
		SequenceNumber: sequenceNumberOfThisDelete,
		IsReplica:      false,
		Replicas:       replicas[1:],
	})

	if err != nil {
		log.Printf("[ Client ][ DeleteFile ]Delete faled for file %v; %v", filename, err)
		return false, err
	} else if r.Status {
		log.Printf("[ Client ][ DeleteFile ]Delete acknowledged by a quorum of replicas for file %v", filename)

		return true, nil

	}

	log.Printf("[ Client ][ DeleteFile ]Delete of file %v Failed", filename)
	return false, errors.New("DeleteFile failed for the file: %v" + filename)
}

func getClientToReplicaServer(replicaIp string) (dn.DataNodeServiceClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")

	dataNodePort := conf.DataNodeServiceSDFSPort
	replica := fmt.Sprintf("%v:%v", replicaIp, dataNodePort)

	log.Printf("Getting the grpc client for the Replica: %v", replica)

	conn, err := grpc.Dial(replica, grpc.WithInsecure())
	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	}

	// defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	// defer cancel()

	// Initialise a client to connect to the coordinator process
	client := dn.NewDataNodeServiceClient(conn)

	return client, ctx, conn, cancel
}

func sendCommitAndInformMasterOfUpdatedVersion(filename string, currentCommittedVersion int64, sequenceNumberOfThisPut int64, replicaIps []string) {
	// send commit to the data nodes
	for idx, replicaIp := range replicaIps {
		isReplica := true
		if idx == 0 {
			isReplica = false
		}
		log.Printf("[ Client ][ PutFile ] Send commit message to replica: %v", replicaIp)
		go sendCommitMessageToReplica(replicaIp, filename, sequenceNumberOfThisPut, isReplica)
	}

	// Tell the master to bump version
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
		log.Printf("Failed to establish connection with the coordinator")
	} else {
		if r.Status {
			log.Printf("[ Client ]Successfully bumped the current committed version of the file on the coordinator")
		}
	}
}

func sendCommitMessageToReplica(replicaIp string, filename string, sequenceNumberOfThisPut int64, isReplica bool) {
	client, ctx, conn, cancel := getClientToReplicaServer(replicaIp)

	defer conn.Close()
	defer cancel()

	log.Printf("[ Client ][ PutFile ]Sending commit message for file %v to replica %v sequenced at %v", filename, replicaIp, sequenceNumberOfThisPut)

	r, err := client.DataNode_CommitFile(ctx, &dn.DataNode_CommitFileRequest{Filename: filename, SequenceNumber: sequenceNumberOfThisPut, IsReplica: isReplica})

	if err != nil {
		log.Printf("[ Client ][ PutFile ]Commit Failed for file %v at replica: %v", filename, replicaIp)
	} else {
		status := r.GetStatus()
		newVersion := r.GetVersion()

		if status == false {
			log.Printf("[ Client ][ PutFile ]Commit Failed after getting response")
		} else {
			log.Printf("[ Client ][ PutFile ]New version after PutFile(%v) committed: %v", filename, newVersion)
		}
	}
}

func sendDeleteCommitAndInformMaster(filename string, sequenceNumberOfThisDelete int64, replicaIps []string) {
	// send commit to the data nodes
	for idx, replicaIp := range replicaIps {
		isReplica := true
		if idx == 0 {
			isReplica = false
		}
		log.Printf("[ Client ][ DeleteFile ] Send delete commit to replica: %v", replicaIp)
		go sendDeleteCommitToReplica(replicaIp, filename, sequenceNumberOfThisDelete, isReplica)
	}

	// Tell the master that delete was successful
	client, ctx, conn, cancel := getClientForCoordinatorService()

	defer conn.Close()
	defer cancel()

	// Call the RPC function on the coordinator process to process the query
	r, err := client.DeleteFileAck(ctx, &cs.CoordinatorDeleteFileAckRequest{
		Filename: filename,
	})

	if err != nil {
		log.Printf("[ Client ][ DeleteFile ]Failed to establish connection with the coordinator")
	} else {
		if r.Status {
			log.Printf("[ Client ][ DeleteFile ]Successfully informed coordinator")
		}
	}
}

func sendDeleteCommitToReplica(replicaIp string, filename string, sequenceNumberOfThisDelete int64, isReplica bool) {
	client, ctx, conn, cancel := getClientToReplicaServer(replicaIp)

	defer conn.Close()
	defer cancel()

	log.Printf("[ Client ][ DeleteFile ]Sending delete commit for file %v to replica %v sequenced at %v", filename, replicaIp, sequenceNumberOfThisDelete)

	r, err := client.DataNode_CommitDelete(ctx, &dn.DataNode_CommitDeleteRequest{Filename: filename, SequenceNumber: sequenceNumberOfThisDelete, IsReplica: isReplica})

	if err != nil {
		log.Printf("[ Client ][ DeleteFile ]Delete Commit Failed for file %v at replica: %v", filename, replicaIp)
	} else {
		status := r.GetStatus()

		if status == false {
			log.Printf("[ Client ][ DeleteFile ]Delete Commit Failed after getting response")
		} else {
			log.Printf("[ Client ][ DeleteFile ]DeleteFile(%v) committed", filename)
		}
	}
}
