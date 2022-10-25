package process

import (
	"bytes"
	"context"
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

func (state *DataNodeState) dataNode_CommitFileChange(filename string) (bool, int) {
	state.Lock()
	defer state.Unlock()

	if _, err := os.Stat("../../sdfs"); os.IsNotExist(err) {
		err := os.Mkdir("../../sdfs", os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating logs folder\n")
		}
	}

	folder_for_the_file := fmt.Sprintf("../../sdfs/%v", filename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		err := os.Mkdir(folder_for_the_file, os.ModePerm)
		if err != nil {
			log.Fatalf("Error creating logs folder\n")
		}
	}

	//update the version
	if _, ok := state.fileVersionMapping[filename]; ok {
		dataNodeState.fileVersionMapping[filename]++
	} else {
		dataNodeState.fileVersionMapping[filename] = 1
	}

	newVersion := state.fileVersionMapping[filename]
	err := os.WriteFile(fmt.Sprintf("%v/%v-%v", folder_for_the_file, filename, newVersion), dataNodeState.preCommitBuffer[filename], 0644)

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

	fileData := bytes.Buffer{}
	startTime := time.Now()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Time taken for the transfer of the file: %v: %vs", filename, int32(endTime.Sub(startTime).Seconds()))

			// go dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation)
			return dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation)
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
		}
	}
}

func (s *DataNodeServer) DataNode_CommitFile(ctx context.Context, in *dn.DataNode_CommitFileRequest) (*dn.DataNode_CommitFileResponse, error) {
	filename := in.GetFilename()
	sequenceNumberForOperation := in.GetSequenceNumber()
	status, version := dataNodeService_CommitFileChanges(filename, int(sequenceNumberForOperation))

	return &dn.DataNode_CommitFileResponse{
		Status:  status,
		Version: int64(version),
	}, nil
}

func dataNode_ProcessFile(filename string, fileData bytes.Buffer, stream dn.DataNodeService_DataNode_PutFileServer, operationSequenceNumber int) error {
	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber() != operationSequenceNumber {
	}

	log.Printf("[DataNode][PutFile] Finally performing PutFile(%v)", filename)

	// Buffering the contents to commit
	dataNodeState.dataNode_AddToPreCommitBuffer(filename, fileData.Bytes())

	// Replicate to other data nodes
	// if quorum reached then ack to the client
	quorum := true
	if quorum {
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: true,
		})
	} else {
		return stream.SendAndClose(&dn.DataNode_PutFile_Response{
			Status: false,
		})
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
