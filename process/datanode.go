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
	sequenceNumber     int

	preCommitBuffer map[string][]byte
}

var (
	dataNodeState *DataNodeState
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

func (state *DataNodeState) dataNode_IncrementSequenceNumber() int {
	state.Lock()
	defer state.Unlock()

	state.sequenceNumber++

	return state.sequenceNumber
}

func (state *DataNodeState) dataNode_GetSequenceNumber() int {
	state.RLock()
	defer state.RUnlock()

	return state.sequenceNumber
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

	dataNodeState.sequenceNumber++

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
	log.Printf("[ DataNode ][ PutFile ]Committing file changes for file: %v sequenced at %v", filename, sequenceNumberForOperation)
	status, version := dataNodeService_CommitFileChanges(filename, int(sequenceNumberForOperation))

	return &dn.DataNode_CommitFileResponse{
		Status:  status,
		Version: int64(version),
	}, nil
}

func dataNode_ProcessFile(filename string, fileData bytes.Buffer, stream dn.DataNodeService_DataNode_PutFileServer, operationSequenceNumber int, replicaNodes []string, isReplica bool, allChunks []*dn.Chunk) error {
	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber() != operationSequenceNumber {
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
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: true,
		})
	} else {
		// quorum wasnt reached so client isnt gonna do a commit
		// increase sequence numbers and tell the other for this file to increase sequence number as well
		// for _, replicaNode := range replicaNodes {
		// 	go dataNode_UpdateSequenceNumberOnReplica(replicaNode, filename, allChunks, replicaChannel)
		// }
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: false,
		})
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
	if dataNodeState.dataNode_GetSequenceNumber() > sequenceNumberForOperation {
		log.Fatalf("Sequence number has gone ahead at the server!! (DataNodeSequence Number: %v, SequenceForOperation: %v)", dataNodeState.dataNode_GetSequenceNumber(), sequenceNumberForOperation)
	}
	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber() != sequenceNumberForOperation {
	}
	return dataNodeState.dataNode_CommitFileChange(filename)
}

func StartDataNodeService_SDFS(port int, wg *sync.WaitGroup) {
	// Initialise the state of the data node
	dataNodeState = &DataNodeState{
		fileVersionMapping: make(map[string]int),
		sequenceNumber:     0,
		preCommitBuffer:    make(map[string][]byte),
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
