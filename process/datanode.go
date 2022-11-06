package process

import (
	"bufio"
	"bytes"
	"context"
	"cs425/mp/config"
	dn "cs425/mp/proto/data_node_proto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type DataNodeState struct {
	sync.RWMutex
	fileVersionMapping map[string]int
	sequenceNumber     map[string]int

	forceUpdateSequenceNumTimer map[string]*time.Timer

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

func (state *DataNodeState) dataNode_SetVersionOfFile(filename string, version int) {
	state.Lock()
	defer state.Unlock()

	state.fileVersionMapping[filename] = version
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

func (state *DataNodeState) dataNode_AddSequenceNumber(filename string, seqNum int) bool {
	state.Lock()
	defer state.Unlock()

	_, ok := state.sequenceNumber[filename]
	if ok {
		log.Fatalf("[ DataNode ][ Replica Recovery ]Already contains the sequence number for the file %v", filename)

		return false
	}
	state.sequenceNumber[filename] = seqNum

	return true

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

func (state *DataNodeState) dataNode_GetPreCommitBufferEntry(filename string) []byte {
	state.RLock()
	defer state.RUnlock()

	return state.preCommitBuffer[filename]
}

func (state *DataNodeState) dataNode_AddToPreCommitBuffer(filename string, data []byte) {
	state.Lock()
	defer state.Unlock()

	state.preCommitBuffer[filename] = data
}

func (state *DataNodeState) dataNode_ClearPreCommitBufferForFile(filename string) {
	state.Lock()
	defer state.Unlock()

	delete(state.preCommitBuffer, filename)
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
	conf := config.GetConfig("../../config/config.json")

	myIpAddr := dataNode_GetOutboundIP()

	if _, err := os.Stat(conf.SDFSDataFolder); os.IsNotExist(err) {
		err := os.Mkdir(conf.SDFSDataFolder, os.ModePerm)
		if err != nil {
			log.Printf("Error creating sdfs folder\n")
		}
	}

	folder_for_the_file := fmt.Sprintf("%v/%v", conf.SDFSDataFolder, filename)

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
	err := os.WriteFile(fmt.Sprintf("%v/%v-%v-%v", folder_for_the_file, filename, newVersion, myIpAddr), dataNodeState.preCommitBuffer[filename], 0644)

	if err != nil {
		log.Fatalf("File writing failed: %v", err)
	}

	dataNodeState.sequenceNumber[filename]++

	delete(state.preCommitBuffer, filename)

	return true, newVersion
}

func (state *DataNodeState) dataNode_DeleteFile(filename string) bool {
	state.Lock()
	defer state.Unlock()

	conf := config.GetConfig("../../config/config.json")

	if _, err := os.Stat(conf.SDFSDataFolder); os.IsNotExist(err) {
		return false
	}

	folder_for_the_file := fmt.Sprintf("%v/%v", conf.SDFSDataFolder, filename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		log.Printf("[ DataNode ][ DeleteFile ]%v doesnt exist", folder_for_the_file)
		return false
	}

	log.Printf("[ DataNode ][ DeleteFile ]Deleting the file %v whose current version is %v", filename, state.fileVersionMapping[filename])

	err := os.RemoveAll(folder_for_the_file)
	if err != nil {
		log.Printf("[ DataNode ][ DeleteFile ]Error deleting file %v; %v", filename, err)

		// return false
	}
	log.Printf("[ DataNode ][ DeleteFile ]Successfully deleted the file %v", filename)

	delete(state.fileVersionMapping, filename)
	delete(state.sequenceNumber, filename)
	delete(state.preCommitBuffer, filename)
	delete(state.forceUpdateSequenceNumTimer, filename)

	return true
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

	fileData := bytes.Buffer{}
	startTime := time.Now()

	var lastChunk *dn.Chunk

	if _, err := os.Stat("../../tmp"); os.IsNotExist(err) {
		err := os.Mkdir("../../tmp", os.ModePerm)
		if err != nil {
			log.Printf("Error creating tmp folder\n")
		}
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Time taken for the transfer of the file: %v: %vs", filename, int32(endTime.Sub(startTime).Seconds()))

			serialisedChunk, _ := json.Marshal(lastChunk)
			os.WriteFile(fmt.Sprintf("../../tmp/%v/chunk-%v", filename, chunkCount), serialisedChunk, 0644)

			// go dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation)
			return dataNode_ProcessFile(filename, fileData, stream, sequenceNumberOfOperation, replicaNodes, isReplica, chunkCount)
		}
		if err != nil {
			return stream.SendAndClose(&dn.DataNode_PutFile_Response{
				Status: false,
			})
		} else {
			log.Printf("Recieved chunk: %v of size: %v", chunk.GetChunkId(), len(chunk.GetChunk()))
			_, err = fileData.Write(chunk.GetChunk())
			if err != nil {
				log.Printf("Chunk aggregation failed - %v", err)
				return err
			}
			if lastChunk != nil {
				serialisedChunk, err := json.Marshal(lastChunk)
				if err != nil {
					log.Panicf("serialization of chunk failed: err: %v", err)
				}
				os.WriteFile(fmt.Sprintf("../../tmp/%v/chunk-%v", filename, chunkCount), serialisedChunk, 0644)
			}
			chunkCount++
			lastChunk = chunk
			if filename == "" {
				filename = chunk.GetFilename()
				folder_for_the_file := fmt.Sprintf("../../tmp/%v", filename)

				if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
					err := os.Mkdir(folder_for_the_file, os.ModePerm)
					if err != nil {
						log.Printf("Error creating folder for file versions\n")
					}
				}
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
	if !isReplica {
		log.Printf("[ Primary Replica ][ PutFile ]Turning off the timer for the file commit")
		// when commit for the file is recieved stop the timer that is there to
		// ensure a failure of commit doesnt block other operations
		dataNodeState.forceUpdateSequenceNumTimer[filename].Stop()
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

	newLocalSequenceNumber := dataNodeState.dataNode_IncrementSequenceNumber(filename)

	if newLocalSequenceNumber == int(newSequenceNumber) {
		log.Printf("[ DataNode ]Upating Sequence Number for file %v: Sequence numbers match: %v", filename, newSequenceNumber)
	} else {
		log.Fatalf("[ DataNode ]Updating Sequence Number for file %v: Sequence number for the file on the node is misaligned. Replica sequence number: %v whereas it should be: %v", filename, newLocalSequenceNumber, newSequenceNumber)
	}

	return &dn.DataNode_UpdateSequenceNumberResponse{}, nil
}

func (s *DataNodeServer) DataNode_InitiateReplicaRecovery(ctx context.Context, in *dn.DataNode_InitiateReplicaRecoveryRequest) (*dn.DataNode_InitiateReplicaRecoveryResponse, error) {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()
	nodeToPullDataFrom := in.GetNodeToReplicateDataFrom()

	client, ctx, conn, cancel := getClientToReplicaServer(nodeToPullDataFrom)
	defer conn.Close()
	defer cancel()

	stream, err := client.DataNode_ReplicaRecovery(ctx, &dn.DataNode_ReplicaRecoveryRequest{
		Filename: filename,
	})

	if err != nil {
		log.Printf("[ DataNode ][ Replica Recovery ]Getting versions file %v for recovery FAILED", filename)

		return &dn.DataNode_InitiateReplicaRecoveryResponse{
			Status: false,
		}, err
	}

	version := 1
	versionData := bytes.Buffer{}
	fileVersions := make(map[int][]byte)
	seqNum := -1
	preCommitBuffer := []byte{} // may be fetch this in another streaming call?
	maxVersion := -1

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if len(versionData.Bytes()) > 0 {
				fileVersions[version] = versionData.Bytes()
				versionData = bytes.Buffer{}
			}
			maxVersion = int(math.Max(float64(maxVersion), float64(version)))
			log.Printf("[ DataNode ][ Replica Recovery ]Got all the chunks of %v versions of the file %v for the replica recovery", len(fileVersions), filename)
			break
		}
		if err != nil {
			return &dn.DataNode_InitiateReplicaRecoveryResponse{
				Status: false,
			}, err
		}
		log.Printf("[ DataNode ][ Replica Recovery ]Receieved the chunk: %v for version %v", chunk.GetChunkId(), chunk.GetVersion())

		if chunk.Version > int64(version) {
			if len(versionData.Bytes()) > 0 {
				fileVersions[version] = versionData.Bytes()
				versionData = bytes.Buffer{}
				version = int(chunk.Version)
			}
			maxVersion = int(math.Max(float64(maxVersion), float64(version)))
		}
		_, err = versionData.Write(chunk.GetChunk())
		if seqNum == -1 {
			seqNum = int(chunk.SequenceNumber)
		}
		if len(chunk.GetPreCommitBuffer()) > 0 && len(preCommitBuffer) == 0 {
			preCommitBuffer = chunk.GetPreCommitBuffer()
		}
		if err != nil {
			log.Panicf("Chunk aggregation failed - %v", err)
			return &dn.DataNode_InitiateReplicaRecoveryResponse{
				Status: false,
			}, err
		}
	}

	// Got all the chunks for all the versions
	// save it to sdfs folder folder

	ok := dataNodeState.dataNode_AddSequenceNumber(filename, seqNum)
	if ok {
		log.Printf("[ DataNode ][ Replica Recovery ]Took a note of the latest sequence number: %v for the file %v", seqNum, filename)
	} else {
		log.Fatalf("[ DataNode ][ Replica Recovery ] Sequence number updation failed")
	}

	dataNodeState.dataNode_SetVersionOfFile(filename, maxVersion)

	if len(preCommitBuffer) > 0 {
		log.Printf("[ DataNode ][ Replica Recovery ]Took a note of the latest buffer changes of the file: %v", filename)
		dataNodeState.dataNode_AddToPreCommitBuffer(filename, preCommitBuffer)
	}

	if _, err := os.Stat(conf.SDFSDataFolder); os.IsNotExist(err) {
		err := os.Mkdir(conf.SDFSDataFolder, os.ModePerm)
		if err != nil {
			log.Printf("Error creating sdfs folder\n")
		}
	}

	folder_for_the_file := fmt.Sprintf("%v/%v", conf.SDFSDataFolder, filename)

	if _, err := os.Stat(folder_for_the_file); os.IsNotExist(err) {
		err := os.Mkdir(folder_for_the_file, os.ModePerm)
		if err != nil {
			log.Printf("Error creating folder for file versions\n")
		}
	}
	myIpAddr := dataNode_GetOutboundIP()
	for v, b := range fileVersions {
		filepath := fmt.Sprintf("%v/%v-%v-%v", folder_for_the_file, filename, v, myIpAddr)
		log.Printf("[ DataNode ][ Replica Recovery ]Writing file %v", filepath)
		err := os.WriteFile(filepath, b, 0644)

		if err != nil {
			log.Fatalf("File writing failed: %v", err)
			return &dn.DataNode_InitiateReplicaRecoveryResponse{
				Status: false,
			}, err
		}

	}

	return &dn.DataNode_InitiateReplicaRecoveryResponse{
		Status: true,
	}, nil
}

func (s *DataNodeServer) DataNode_ReplicaRecovery(in *dn.DataNode_ReplicaRecoveryRequest, stream dn.DataNodeService_DataNode_ReplicaRecoveryServer) error {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()

	log.Printf("[ DataNode ][ Replica Recovery ]DataNode_ReplicaRecovery - Filename: %v", filename)

	fileFolder := fmt.Sprintf("%v/%v", conf.SDFSDataFolder, filename)
	files, err := ioutil.ReadDir(fileFolder)
	if err != nil {
		log.Printf("[ DataNode ][ Replica Recovery ]SDFS directory (%v) read failed: %v", fileFolder, err)
		return err
	}

	for _, f := range files {
		log.Printf("[ DataNode ][ Replica Recovery ]Sending file: %v", f.Name())
		fName := strings.Split(f.Name(), "-")[0]

		if fName != filename {
			continue
		}
		fVersion, _ := strconv.Atoi(strings.Split(f.Name(), "-")[1])

		if f.Name() != fmt.Sprintf("%v-%v-%v", fName, fVersion, dataNode_GetOutboundIP()) {
			continue
		}

		filePath := fmt.Sprintf("%v/%v/%v", conf.SDFSDataFolder, filename, f.Name())

		log.Printf("[ DataNode ][ Replica Recovery ]Sending the file: %v", filePath)

		file, err := os.Open(filePath)

		if err != nil {
			// fmt.Printf("File %v doesn't exist :(", fName)
			log.Fatalf("[ DataNode ][ Replica Recovery ]Cannot open File: %v - %v", filePath, err)
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
				log.Fatalf("[ DataNode ][ Replica Recovery ]Cannot read chunk to buffer: %v", err)
			}

			req := &dn.FileChunk{
				ChunkId:         int64(chunkId),
				Filename:        fName,
				Version:         int64(fVersion),
				Chunk:           buffer[:n],
				SequenceNumber:  int64(dataNodeState.dataNode_GetSequenceNumber(filename)),
				PreCommitBuffer: dataNodeState.dataNode_GetPreCommitBufferEntry(filename),
			}
			log.Printf("[ DataNode ][ Replica Recovery ]Sending chunk %v of file: %v", chunkId, fName)
			e := stream.Send(req)
			if e != nil {
				log.Fatalf("[ DataNode ][ Replica Recovery ]Cannot send chunk %v of file %v to dataNode: %v", chunkId, fName, e)
			}
			chunkId++
		}
	}
	log.Printf("[ DataNode ][ Replica Recovery ]Sent all the chunks of the file %v", filename)
	return nil
}

func dataNode_ProcessFile(filename string, fileData bytes.Buffer, stream dn.DataNodeService_DataNode_PutFileServer, operationSequenceNumber int, replicaNodes []string, isReplica bool, chunkCount int) error {
	// wait until sequence number
	log.Printf("[ DataNode ][ PutFile ]Received Write with sequence number %v and local sequence number for that file is %v", operationSequenceNumber, dataNodeState.dataNode_GetSequenceNumber(filename))
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
	log.Printf("[ Primary Replica ][ PutFile ]Replicating the file on the replica nodes")
	return dataNode_Replicate(filename, chunkCount, replicaNodes, stream)
}

func dataNode_Replicate(filename string, chunkCount int, replicaNodes []string, stream dn.DataNodeService_DataNode_PutFileServer) error {
	conf := config.GetConfig("../../config/config.json")
	// concurrently send chunks to all the replicas
	replicaChannel := make(chan bool)
	quorum := false
	quorumCount := 1
	for _, replicaNode := range replicaNodes {
		go dataNode_SendFileToReplica(replicaNode, filename, replicaChannel, chunkCount)
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
		log.Printf("[ Primary Replica ][ PutFile ]Incrementing sequence number and asking all the replica nodes to increment as well for this file to unblock other operations on this file")
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
	log.Printf("[ Primary Replica ][ PutFile/DeleteFile ]Starting timer for the commit of the file: %v", filename)
	dataNodeState.forceUpdateSequenceNumTimer[filename] = time.NewTimer(time.Duration(COMMIT_TIMEOUT) * time.Second)

	<-dataNodeState.forceUpdateSequenceNumTimer[filename].C
	log.Printf("[ Primary Replica ][ PutFile/DeleteFile ]Did not recieve a commit for the file %v in %v seconds and hence updating the sequence number which is currently %v", filename, COMMIT_TIMEOUT, dataNodeState.dataNode_GetSequenceNumber(filename))
	seqNum := dataNodeState.dataNode_IncrementSequenceNumber(filename)
	for _, replicaNode := range replicaNodes {
		go dataNode_UpdateSequenceNumberOnReplica(replicaNode, filename, seqNum)
	}
}

func dataNode_UpdateSequenceNumberOnReplica(replica string, filename string, newSequenceNumber int) {
	client, ctx, conn, cancel := getClientToReplicaServer(replica)
	defer conn.Close()
	defer cancel()

	log.Printf("[ DataNode ][ PutFile/DeleteFile ]File changes for file: %v can't be committed and so let us update the sequence number so that other operations can proceed", filename)

	_, err := client.DataNode_UpdateSequenceNumber(ctx, &dn.DataNode_UpdateSequenceNumberRequest{Filename: filename, SequenceNumber: int64(newSequenceNumber)})

	if err != nil {
		log.Printf("[ Primary Replica ][ PutFile/DeleteFile ]Updating sequence number on the replica: %v errored: %v", replica, err)
	} else {
		log.Printf("[ Primary Replica ][ PutFile/DeleteFile ]Updating sequence number on the replica: %v success", replica)
	}
}

func dataNode_SendFileToReplica(replica string, filename string, replicaChannel chan bool, chunkCount int) {
	client, ctx, conn, cancel := getClientToReplicaServer(replica)
	defer conn.Close()
	defer cancel()

	stream, streamErr := client.DataNode_PutFile(ctx)
	if streamErr != nil {
		log.Printf("Cannot upload File: %v", streamErr)
	}

	for i := 1; i <= chunkCount; i++ {
		chunkFile := fmt.Sprintf("../../tmp/%v/chunk-%v", filename, i)
		c, err := os.ReadFile(chunkFile)
		if err != nil {
			log.Panicf("Issue reading tmp file chunk: %v", chunkFile)
		}
		var chunk *dn.Chunk

		unmarshallingError := json.Unmarshal(c, &chunk)
		if unmarshallingError != nil {
			log.Printf("[ Primary Replica ][ Replicate ]Error while unmarshalling the chunk file: %v; error: %v\n", chunkFile, unmarshallingError)
		}

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
		log.Printf("[ Primary Replica ][ Replicate ]Replication of file %v failed for the replica: %v", filename, replica)
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

func DataNode_ListAllFilesOnTheNode() []string {
	// conf := config.GetConfig("../../config/config.json")

	files, err := ioutil.ReadDir("../../sdfs")
	if err != nil {
		log.Printf("[ DataNode ][ ListFilesOnNode ]This node does not contain any file")
		return []string{}
	}
	filenames := []string{}

	for _, file := range files {
		if file.Name()[0] == '.' {
			continue
		}
		filenames = append(filenames, file.Name())
	}

	return filenames
}

func StartDataNodeService_SDFS(port int, wg *sync.WaitGroup) {
	// Initialise the state of the data node
	dataNodeState = &DataNodeState{
		fileVersionMapping:          make(map[string]int),
		sequenceNumber:              make(map[string]int),
		preCommitBuffer:             make(map[string][]byte),
		forceUpdateSequenceNumTimer: make(map[string]*time.Timer),
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

func (s *DataNodeServer) DataNode_GetFile(in *dn.DataNode_GetFileRequest, stream dn.DataNodeService_DataNode_GetFileServer) error {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()
	sequenceNum := int(in.GetSequenceNumber())
	replicas := in.GetReplicas()
	version := in.GetVersion()

	// // wait until sequence number
	// for dataNodeState.dataNode_GetSequenceNumber(filename) != sequenceNum {
	// }

	fileVersionOnNode, ok := dataNodeState.dataNode_GetVersionOfFile(filename)
	if !ok {
		log.Printf("[ Primary Replica ][ GetFile ]The primary replica doesnt contain the file: %v", filename)
		return errors.New("The primary replica doesnt contain the file")
	}
	if fileVersionOnNode < int(version) {
		log.Printf("[ DataNode ][ GetFile ]The primary replica doesnt have the version requested for %v; Version on node: %v; Version requested: %v", filename, fileVersionOnNode, version)
		return errors.New("]The primary replica doesnt have the version requested")
	}

	replicaChannel := make(chan bool)
	quorum := false
	quorumCount := 1
	for _, replicaNode := range replicas {
		go dataNode_GetFileQuorumFromReplica(replicaNode, filename, version, replicaChannel)
	}
	for {
		status := <-replicaChannel
		if status {
			quorumCount++
			log.Printf("[ Primary Replica ][ GetFile ]Receieved a read success from a replica; Current quorum: %v", quorumCount)
		}
		if quorumCount >= conf.ReadQuorum {
			log.Printf("[ Primary Replica ][ GetFile ]Quorum achieved")
			quorum = true
			break
		}
	}
	if quorum {
		// increase sequence number on all the replica nodes
		return sendFileToClient(filename, int(version), stream, sequenceNum)
	} else {
		// increase sequence number on all the replica nodes
		return errors.New("Quorum not obtained")
	}

}

func (s *DataNodeServer) DataNode_GetFileVersions(in *dn.DataNode_GetFileVersionsRequest, stream dn.DataNodeService_DataNode_GetFileVersionsServer) error {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()
	replicas := in.GetReplicas()
	version := in.GetVersion()
	numVersions := in.GetNumVersions()

	fileVersionOnNode, ok := dataNodeState.dataNode_GetVersionOfFile(filename)
	if !ok {
		log.Printf("[ Primary Replica ][ GetFileVersions ]The primary replica doesnt contain the file: %v", filename)
		return errors.New("The primary replica doesnt contain the file")
	}
	if fileVersionOnNode < int(version) {
		log.Printf("[ DataNode ][ GetFileVersions ]The primary replica doesnt have the version requested for %v; Version on node: %v; Version requested: %v", filename, fileVersionOnNode, version)
		return errors.New("]The primary replica doesnt have the version requested")
	}

	replicaChannel := make(chan bool)
	quorum := false
	quorumCount := 1
	for _, replicaNode := range replicas {
		go dataNode_GetFileQuorumFromReplica(replicaNode, filename, version, replicaChannel)
	}
	for {
		status := <-replicaChannel
		if status {
			quorumCount++
			log.Printf("[ Primary Replica ][ GetFileVersions ]Receieved a read success from a replica; Current quorum: %v", quorumCount)
		}
		if quorumCount >= conf.ReadQuorum {
			log.Printf("[ Primary Replica ][ GetFileVersions ]Quorum achieved")
			quorum = true
			break
		}
	}
	if quorum {
		// increase sequence number on all the replica nodes
		return sendFileVersionsToClient(filename, int(version), stream, int(numVersions))
	} else {
		// increase sequence number on all the replica nodes
		return errors.New("Quorum not obtained")
	}

}

func sendFileToClient(filename string, version int, stream dn.DataNodeService_DataNode_GetFileServer, sequenceNum int) error {
	conf := config.GetConfig("../../config/config.json")
	filePath := fmt.Sprintf("%v/%v/%v-%v-%v", conf.SDFSDataFolder, filename, filename, version, dataNode_GetOutboundIP())

	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("cannot open File: %v - %v", filePath, err)
		return errors.New("[ Primary Replica ][ GetFile ]Cannot open the file: " + filename)

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
			return errors.New("[ Primary Replica ][ GetFile ]Cannot read chunk to the buffer; Filename: " + filename)
		}
		req := &dn.FileChunk{
			ChunkId:        int64(chunkId),
			Filename:       filename,
			Version:        int64(version),
			Chunk:          buffer[:n],
			SequenceNumber: int64(sequenceNum),
		}
		log.Printf("[ Primary Replica ][ GetFile ]Sending chunk %v of file: %v with version: %v", chunkId, filename, version)
		e := stream.Send(req)
		if e != nil {
			log.Fatalf("[ Primary Replica ][ GetFile ]Cannot send chunk %v of file %v with version: %v to dataNode: %v", chunkId, filename, version, e)
			return errors.New("Cannot send chunk of file: " + filename)
		}
		chunkId++
	}
	return nil
}

func sendFileVersionsToClient(filename string, version int, stream dn.DataNodeService_DataNode_GetFileVersionsServer, numVersions int) error {
	conf := config.GetConfig("../../config/config.json")
	l := int(math.Max(float64(version-numVersions+1), 1.0))

	for v := version; v >= l; v-- {
		filePath := fmt.Sprintf("%v/%v/%v-%v-%v", conf.SDFSDataFolder, filename, filename, v, dataNode_GetOutboundIP())

		file, err := os.Open(filePath)

		if err != nil {
			log.Fatalf("cannot open File: %v - %v", filePath, err)
			return errors.New("[ Primary Replica ][ GetFileVersions ]Cannot open the file: " + filename)
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		buffer := make([]byte, conf.ChunkSize)
		versionString := fmt.Sprintf("_____________________________Version - %v_____________________________\n\n", v)
		vs := []byte(versionString)
		chunkId := 0

		for {
			n, err := reader.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("cannot read chunk to buffer: %v", err)
				return errors.New("[ Primary Replica ][ GetFileVersions ]Cannot read chunk to the buffer; Filename: " + filename)
			}
			bufferToSend := buffer[:n]
			if chunkId == 0 {
				newBuffer := []byte(vs)
				newBuffer = append(newBuffer, bufferToSend...)
				bufferToSend = newBuffer
			}

			req := &dn.FileChunk{
				ChunkId:  int64(chunkId),
				Filename: filename,
				Version:  int64(v),
				Chunk:    bufferToSend,
			}
			log.Printf("[ Primary Replica ][ GetFileVersions ]Sending chunk %v of file: %v with version: %v", chunkId, filename, v)
			e := stream.Send(req)
			if e != nil {
				log.Fatalf("[ Primary Replica ][ GetFileVersions ]Cannot send chunk %v of file %v with version: %v to dataNode: %v", chunkId, filename, v, e)
				return errors.New("Cannot send chunk of file: " + filename)
			}
			chunkId++
		}
	}

	return nil
}

func dataNode_GetFileQuorumFromReplica(replica string, filename string, version int64, replicaChannel chan bool) {
	client, ctx, conn, cancel := getClientToReplicaServer(replica)
	defer conn.Close()
	defer cancel()

	_, err := client.DataNode_GetFileQuorum(ctx, &dn.DataNode_GetFileQuorumRequest{
		Filename: filename,
		Version:  version,
	})

	if err != nil {
		log.Printf("[ Primary Replica ][ GetFile ]Error getting quorum for GetFile(%v) from replica: %v; Error: %v", filename, replica, err)
		replicaChannel <- false
	} else {
		log.Printf("[ Primary Replica ][ GetFile ]GetFile(%v) on replica %v successful", filename, replica)
		replicaChannel <- true
	}
}

func (s *DataNodeServer) DataNode_GetFileQuorum(ctx context.Context, in *dn.DataNode_GetFileQuorumRequest) (*dn.DataNode_GetFileQuorumResponse, error) {
	filename := in.GetFilename()
	version := int(in.GetVersion())
	fileVersionOnNode, ok := dataNodeState.dataNode_GetVersionOfFile(filename)
	if !ok {
		log.Printf("[ DataNode ][ GetFile ]The replica doesnt contain the file: %v", filename)
		return &dn.DataNode_GetFileQuorumResponse{
			Status: false,
		}, errors.New("The replica doesnt contain the file")
	}
	log.Printf("[ DataNode ][ GetFile ]Version of the file on the datanode: %v; Version of the file requested: %v", fileVersionOnNode, version)

	if fileVersionOnNode >= version {
		return &dn.DataNode_GetFileQuorumResponse{
			Status: true,
		}, nil
	}

	return &dn.DataNode_GetFileQuorumResponse{
		Status: false,
	}, errors.New("The replica doesnt have that ")
}

func (s *DataNodeServer) DataNode_DeleteFileQuorumCheck(ctx context.Context, in *dn.DataNode_DeleteFileQuorumCheckRequest) (*dn.DataNode_DeleteFileQuorumCheckResponse, error) {
	conf := config.GetConfig("../../config/config.json")
	filename := in.GetFilename()
	sequenceNumber := in.GetSequenceNumber()
	isReplica := in.GetIsReplica()

	log.Printf("[ DataNode][ DeleteFile ]Checking for Delete Quorum for file %v", filename)

	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber(filename) != int(sequenceNumber) {
	}

	_, ok := dataNodeState.dataNode_GetVersionOfFile(filename)
	if !ok {
		log.Printf("[ DataNode ][ DeleteFile ]The replica doesn't contain the file: %v", filename)
		return &dn.DataNode_DeleteFileQuorumCheckResponse{
			Status: false,
		}, errors.New("The replica doesnt contain the file")
	} else if isReplica {
		return &dn.DataNode_DeleteFileQuorumCheckResponse{
			Status: true,
		}, nil
	}

	replicas := in.GetReplicas()

	if len(replicas) <= 0 {
		log.Printf("[ Primary Replica ][ DeleteFile ]Oops no replicas")
		return &dn.DataNode_DeleteFileQuorumCheckResponse{
			Status: false,
		}, errors.New("No replica nodes found for the file " + filename)
	}

	replicaChannel := make(chan bool)
	quorum := false
	quorumCount := 1
	for _, replicaNode := range replicas {
		go dataNode_CheckWithReplicaForDelete(replicaNode, filename, sequenceNumber, replicaChannel)
	}
	for {
		status := <-replicaChannel
		if status {
			quorumCount++
			log.Printf("[ Primary Replica ][ DeleteFile ]Receieved a delete success from a replica; Current quorum: %v", quorumCount)
		}
		if quorumCount >= conf.WriteQuorum {
			log.Printf("[ Primary Replica ][ DeleteFile ]Quorum achieved")
			quorum = true
			break
		}
	}

	if quorum {
		// start a timer
		// if the client sends commit in that timer time -> cool
		// else increase the sequence number and discard the updates

		go dataNode_HandleNoCommits(replicas, filename)

		return &dn.DataNode_DeleteFileQuorumCheckResponse{
			Status: true,
		}, nil
	} else {
		// quorum wasnt reached so client isnt gonna do a commit
		// increase sequence numbers and tell the other for this file to increase sequence number as well
		log.Printf("[ Primary Replica ][ DeleteFile ]Incrementing sequence number and asking all the replica nodes to increment as well for this file to unblock other operations on this file")
		seqNum := dataNodeState.dataNode_IncrementSequenceNumber(filename)
		for _, replicaNode := range replicas {
			go dataNode_UpdateSequenceNumberOnReplica(replicaNode, filename, seqNum)
		}
		return &dn.DataNode_DeleteFileQuorumCheckResponse{
			Status: false,
		}, nil
	}
}

func dataNode_CheckWithReplicaForDelete(replica string, filename string, sequenceNumber int64, replicaChannel chan bool) {
	log.Printf("[ Primary Replica ][ DeleteFile ]Checking quorum for delete with replica: %v", replica)
	client, ctx, conn, cancel := getClientToReplicaServer(replica) // currently always picking the first allocated node as the primary replica
	defer conn.Close()
	defer cancel()

	r, err := client.DataNode_DeleteFileQuorumCheck(ctx, &dn.DataNode_DeleteFileQuorumCheckRequest{
		Filename:       filename,
		SequenceNumber: sequenceNumber,
		IsReplica:      true,
	})

	if err != nil {
		log.Printf("[ Primary Replica ][ DeleteFile ]Delete Quorum check failed for replica %v", replica)

		replicaChannel <- false
	} else {
		if r.Status {
			log.Printf("[ Primary Replica ][ DeleteFile ]Delete acknowledged by replica %v", replica)
			replicaChannel <- true
		} else {
			log.Printf("[ Primary Replica ][ DeleteFile ]Delete not acknowledged by replica %v", replica)
			replicaChannel <- false
		}
	}
}

func (s *DataNodeServer) DataNode_CommitDelete(ctx context.Context, in *dn.DataNode_CommitDeleteRequest) (*dn.DataNode_CommitDeleteResponse, error) {
	filename := in.GetFilename()
	sequenceNumber := in.GetSequenceNumber()
	isReplica := in.GetIsReplica()

	// wait until sequence number
	for dataNodeState.dataNode_GetSequenceNumber(filename) != int(sequenceNumber) {
	}

	if !isReplica {
		log.Printf("[ Primary Replica ][ DeleteFile ]Turning off the timer for the file commit")
		// when commit for the file is recieved stop the timer that is there to
		// ensure a failure of commit doesnt block other operations
		dataNodeState.forceUpdateSequenceNumTimer[filename].Stop()
	}

	log.Printf("[ DataNode ][ DeleteFile ]Committing the delete operation of file %v", filename)

	status := dataNodeState.dataNode_DeleteFile(filename)

	return &dn.DataNode_CommitDeleteResponse{
		Status: status,
	}, nil
}
