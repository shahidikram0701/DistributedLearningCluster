package process

import (
	"bytes"
	"context"
	"cs425/mp/config"
	dn "cs425/mp/proto/data_node_proto"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type DataNodeState struct {
	sync.RWMutex
	fileVersionMapping map[string]int
	sequenceNumber     map[string]int

	forceUpdateSequenceNumTimer *time.Timer

	preCommitBuffer map[string][]byte
}

var (
	dataNodeState *DataNodeState
)

var (
	COMMIT_TIMEOUT = 5 // second
)

func (state *DataNodeState) dataNode_GetVersionOfFile(filename string) (int, bool) {
	state.RLock()
	defer state.RUnlock()

	v, ok := state.fileVersionMapping[filename]

	return v, ok
}

func (state *DataNodeState) dataNode_InitialiseVersionOfFile(filename string) int {
	state.Lock()
	defer state.Unlock()

	state.fileVersionMapping[filename] = 1

	return state.fileVersionMapping[filename]
}

func (state *DataNodeState) dataNode_IncrementVersionOfFile(filename string) int {
	state.Lock()
	defer state.Unlock()

	state.fileVersionMapping[filename]++

	return state.fileVersionMapping[filename]
}

func (state *DataNodeState) dataNode_IncrementSequenceNumber(filename string) int {
	state.Lock()
	defer state.Unlock()

	_, ok := state.sequenceNumber[filename]

	if !ok {
		state.sequenceNumber[filename] = 0
	}

	state.sequenceNumber[filename]++

	return state.sequenceNumber[filename]
}

func (state *DataNodeState) dataNode_GetSequenceNumber(filename string) int {
	state.RLock()
	defer state.RUnlock()

	seqNum, ok := state.sequenceNumber[filename]

	if !ok {
		seqNum = 0
		state.sequenceNumber[filename] = 0
	}

	return seqNum
}

func (state *DataNodeState) dataNode_AddToPreCommitBuffer(filename string, data []byte) {
	state.Lock()
	defer state.Unlock()

	state.preCommitBuffer[filename] = data
}

func dataNode_GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Couldn't get the IP address of the process\n%v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func (state *DataNodeState) dataNode_CommitFileChange(filename string) (bool, int) {
	state.Lock()
	defer state.Unlock()

	myIpAddr := dataNode_GetOutboundIP()

	if _, err := os.Stat("../../sdfs"); os.IsNotExist(err) {
		err := os.Mkdir("../../sdfs", os.ModePerm)
		if err != nil {
			log.Printf("Error creating sdfs folder\n")
		}
	}

	folder_for_the_file := fmt.Sprintf("../../sdfs/%v", filename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		err := os.Mkdir(folder_for_the_file, os.ModePerm)
		if err != nil {
			log.Printf("Error creating folder for file versions\n")
		}
	}

	//update the version
	if _, ok := state.fileVersionMapping[filename]; ok {
		dataNodeState.fileVersionMapping[filename]++
	} else {
		dataNodeState.fileVersionMapping[filename] = 1
	}

	newVersion := state.fileVersionMapping[filename]
	err := os.WriteFile(fmt.Sprintf("%v/%v-%v_%v", folder_for_the_file, filename, newVersion, myIpAddr), dataNodeState.preCommitBuffer[filename], 0644)

	if err != nil {
		log.Fatalf("File writing failed: %v", err)
	}

	dataNodeState.sequenceNumber[filename]++

	return true, newVersion
}

type DataNodeServer struct {
	dn.UnimplementedDataNodeServiceServer
}

func (s *DataNodeServer) DataNode_PutFile(stream dn.DataNodeService_DataNode_PutFileServer) error {
	var chunkCount int
	var filename string
	version := -1
	var filesize int
	var replicaNodes []string
	sequenceNumberOfOperation := -1
	var isReplica bool
	allChunks := []*dn.Chunk{}

	fileData := bytes.Buffer{}
	startTime := time.Now()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Time taken for the transfer of the file: %v: %vs", filename, int32(endTime.Sub(startTime).Seconds()))

			// go dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation)
			return dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation, replicaNodes, isReplica, allChunks)
		}
		if err != nil {
			return stream.SendAndClose(&dn.DataNode_PutFile_Response{
				Status: false,
			})
		} else {
			log.Printf("Recieved chunk: %v of size: %v", chunk.GetChunkId(), len(chunk.GetChunk()))
			_, err = fileData.Write(chunk.GetChunk())
			if err != nil {
				log.Panicf("Chunk aggregation failed - %v", err)
			}
			allChunks = append(allChunks, chunk)
			chunkCount++
			if filename == "" {
				filename = chunk.GetFilename()
			}
			if replicaNodes == nil {
				replicaNodes = chunk.GetReplicaNodes()
			}
			if version == -1 {
				version = int(chunk.GetVersion())
			}
			if filesize == 0 {
				filesize = int(chunk.GetFilesize())
			}
			if sequenceNumberOfOperation == -1 {
				sequenceNumberOfOperation = int(chunk.GetSequenceNumber())
			}
			isReplica = chunk.GetIsReplicaChunk()
		}
	}
}

func (s *DataNodeServer) DataNode_CommitFile(ctx context.Context, in *dn.DataNode_CommitFileRequest) (*dn.DataNode_CommitFileResponse, error) {
	filename := in.GetFilename()
	sequenceNumberForOperation := in.GetSequenceNumber()
	isReplica := in.IsReplica
	log.Printf("[ DataNode ][ PutFile ]Committing file changes for file: %v sequenced at %v", filename, sequenceNumberForOperation)
	log.Printf("[ DataNode ][ PutFile ]Turning off the timer for the file commit")
	// when commit for the file is recieved stop the timer that is there to
	// ensure a failure of commit doesnt block other operations
	if !isReplica {
		dataNodeState.forceUpdateSequenceNumTimer.Stop()
	}
	status, version := dataNodeService_CommitFileChanges(filename, int(sequenceNumberForOperation))

	return &dn.DataNode_CommitFileResponse{
		Status:  status,
		Version: int64(version),
	}, nil
}

func (s *DataNodeServer) DataNode_UpdateSequenceNumber(ctx context.Context, in *dn.DataNode_UpdateSequenceNumberRequest) (*dn.DataNode_UpdateSequenceNumberResponse, error) {
	filename := in.GetFilename()
	newSequenceNumber := in.GetSequenceNumber()
	log.Printf("[ DataNode ][ PutFile ]File changes for file: %v can't be committed and so let us update the sequence number so that other operations can proceed", filename)

	newLocalSequenceNumber := dataNodeState.dataNode_IncrementSequenceNumber(filename)

	if newLocalSequenceNumber == int(newSequenceNumber) {
		log.Printf("[ DataNode ][ PutFile ]Replica and Primary are in sync. Sequence numbers match: %v", newSequenceNumber)
	} else {
		log.Fatalf("[ DataNode ][ PutFile ]Replica and Primary are out of sync. Replica sequence number: %v and Primary's sequence number: %v", newLocalSequenceNumber, newSequenceNumber)
	}

	return &dn.DataNode_UpdateSequenceNumberResponse{}, nil
}

func dataNode_ProcessFile(filename string, fileData bytes.Buffer, stream dn.DataNodeService_DataNode_PutFileServer, operationSequenceNumber int, replicaNodes []string, isReplica bool, allChunks []*dn.Chunk) error {
	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber(filename) != operationSequenceNumber {
	}

	log.Printf("[ DataNode ][ PutFile ]Finally performing PutFile(%v)", filename)

	log.Printf("[ DataNode ][ PutFile ]Buffering the PutFile(%v)", filename)

	// Buffering the contents to commit
	dataNodeState.dataNode_AddToPreCommitBuffer(filename, fileData.Bytes())

	if isReplica {
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: true,
		})
	}
	// Replicate to other data nodes
	return dataNode_Replicate(filename, allChunks, replicaNodes, stream)
}

func dataNode_Replicate(filename string, allChunks []*dn.Chunk, replicaNodes []string, stream dn.DataNodeService_DataNode_PutFileServer) error {
	conf := config.GetConfig("../../config/config.json")
	// concurrently send chunks to all the replicas
	replicaChannel := make(chan bool)
	quorum := false
	quorumCount := 1
	for _, replicaNode := range replicaNodes {
		go dataNode_SendFileToReplica(replicaNode, filename, allChunks, replicaChannel)
	}
	for {
		status := <-replicaChannel
		if status {
			quorumCount++
			log.Printf("[ Primary Replica ][ PutFile ]Receieved a write success from a replica; Current quorum: %v", quorumCount)
		}
		if quorumCount >= conf.WriteQuorum {
			log.Printf("[ Primary Replica ][ PutFile ]Quorum achieved")
			quorum = true
			break
		}
	}

	if quorum {
		// start a timer
		// if the client sends commit in that timer time -> cool
		// else increase the sequence number and discard the buffered updates

		go dataNode_HandleNoCommits(replicaNodes, filename)

		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: true,
		})
	} else {
		// quorum wasnt reached so client isnt gonna do a commit
		// increase sequence numbers and tell the other for this file to increase sequence number as well
		seqNum := dataNodeState.dataNode_IncrementSequenceNumber(filename)
		for _, replicaNode := range replicaNodes {
			go dataNode_UpdateSequenceNumberOnReplica(replicaNode, filename, seqNum)
		}
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: false,
		})
	}
}

func dataNode_HandleNoCommits(replicaNodes []string, filename string) {
	log.Printf("[ Primary Replica ][ PutFile ]Starting timer for the commit of the file: %v", filename)
	dataNodeState.forceUpdateSequenceNumTimer = time.NewTimer(time.Duration(COMMIT_TIMEOUT) * time.Second)

	<-dataNodeState.forceUpdateSequenceNumTimer.C
	log.Printf("[ Primary Replica ][ PutFile ]Did not recieve a commit for the file %v in %v seconds and hence updating the sequence number which is currently %v", filename, COMMIT_TIMEOUT, dataNodeState.dataNode_GetSequenceNumber(filename))
	seqNum := dataNodeState.dataNode_IncrementSequenceNumber(filename)
	for _, replicaNode := range replicaNodes {
		go dataNode_UpdateSequenceNumberOnReplica(replicaNode, filename, seqNum)
	}
}

func dataNode_UpdateSequenceNumberOnReplica(replica string, filename string, newSequenceNumber int) {
	client, ctx, conn, cancel := getClientToReplicaServer(replica)
	defer conn.Close()
	defer cancel()

	_, err := client.DataNode_UpdateSequenceNumber(ctx, &dn.DataNode_UpdateSequenceNumberRequest{Filename: filename, SequenceNumber: int64(newSequenceNumber)})

	if err != nil {
		log.Printf("[ Primary Replica ][ PutFile ]Updating sequence number on the replica: %v errored: %v", replica, err)
	} else {
		log.Printf("[ Primary Replica ][ PutFile ]Updating sequence number on the replica: %v success", replica)
	}
}

func dataNode_SendFileToReplica(replica string, filename string, allChunks []*dn.Chunk, replicaChannel chan bool) {
	client, ctx, conn, cancel := getClientToReplicaServer(replica)
	defer conn.Close()
	defer cancel()

	stream, streamErr := client.DataNode_PutFile(ctx)
	if streamErr != nil {
		log.Printf("Cannot upload File: %v", streamErr)
	}

	for _, chunk := range allChunks {
		chunk.IsReplicaChunk = true
		req := chunk
		log.Printf("[ Primary Replica ][ Replicate ]Replicate chunk %v of file: %v to replica: %v", req.ChunkId, filename, replica)
		e := stream.Send(req)
		if e != nil {
			log.Fatalf("[ Primary Replica ][ Replicate ]Cannot send chunk %v of file %v to replica: %v --- %v", req.ChunkId, filename, replica, e)
		}
	}

	res, err := stream.CloseAndRecv()

	if err != nil || res.Status == false {
		log.Fatalf("[ Primary Replica ][ Replicate ]Replication of file %v failed for the replica: %v", filename, replica)
		replicaChannel <- false
	} else {
		log.Printf("[ Primary Replica ][ Replicate ]Replication of file %v succeeded for the replica: %v", filename, replica)
		replicaChannel <- true
	}
}

func dataNodeService_CommitFileChanges(filename string, sequenceNumberForOperation int) (bool, int) {
	if dataNodeState.dataNode_GetSequenceNumber(filename) > sequenceNumberForOperation {
		log.Fatalf("Sequence number has gone ahead at the server!! (DataNodeSequence Number: %v, SequenceForOperation: %v)", dataNodeState.dataNode_GetSequenceNumber(filename), sequenceNumberForOperation)
	}
	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber(filename) != sequenceNumberForOperation {
	}
	return dataNodeState.dataNode_CommitFileChange(filename)
}

func StartDataNodeService_SDFS(port int, wg *sync.WaitGroup) {
	// Initialise the state of the data node
	dataNodeState = &DataNodeState{
		fileVersionMapping:          make(map[string]int),
		sequenceNumber:              make(map[string]int),
		preCommitBuffer:             make(map[string][]byte),
		forceUpdateSequenceNumTimer: nil,
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	dn.RegisterDataNodeServiceServer(s, &DataNodeServer{})

	// cs.RegisterCoordinatorServiceForSDFSServer(s, &CoordinatorServerForSDFS{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}
