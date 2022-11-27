package process

import (
	"bufio"
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	ws "cs425/mp/proto/worker_proto"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type WorkerState struct {
	sync.RWMutex
	id         string
	workerPort int
	models     map[string]int
}

var (
	workerState *WorkerState
)

func (state *WorkerState) SetId(ip string) {
	state.Lock()
	defer state.Unlock()

	state.id = ip
}

func (state *WorkerState) RecordModel(modelId string) int {
	state.Lock()
	defer state.Unlock()

	if _, ok := state.models[modelId]; ok {
		log.Fatalf("[ Worker ][ DeployModel ]Model already exists")
	}

	port := state.workerPort
	state.models[modelId] = port

	state.workerPort++

	return port
}

func (state *WorkerState) GetModelPort(modelId string) (int, bool) {
	state.RLock()
	defer state.RUnlock()

	if port, ok := state.models[modelId]; ok {
		return port, ok
	}

	return -1, false
}

func (state *WorkerState) GetId() string {
	return state.id
}

type WorkerService struct {
	ws.UnimplementedWorkerServiceServer
}

func StartWorkerService(port int, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	ws.RegisterWorkerServiceServer(s, &WorkerService{})

	// Initialise the worker state
	workerState = &WorkerState{
		id:         fmt.Sprintf("%v", dataNode_GetOutboundIP()),
		workerPort: 8000,
		models:     make(map[string]int),
	}

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		wg.Done()
	}
}

func (s *WorkerService) SetupModel(ctx context.Context, in *ws.SetupModelRequest) (*ws.SetupModelReply, error) {
	conf := config.GetConfig("../../config/config.json")
	modelId := in.GetModelId()

	weightFile := fmt.Sprintf("%v.weights.h5", modelId)
	codeFile := fmt.Sprintf("%v.py", modelId)

	if !GetFile(weightFile, weightFile, true) {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Fetching the model weights failed")
		return &ws.SetupModelReply{
			Status: false,
		}, errors.New("Fetching of model weights failed")
	}

	if !GetFile(codeFile, codeFile, true) {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Fetching the model code failed")
		return &ws.SetupModelReply{
			Status: false,
		}, errors.New("Fetching of model code failed")
	}

	modelFolder := fmt.Sprintf("%v/%v", conf.SDFSModelsFolder, modelId)
	if _, err := os.Stat(modelFolder); os.IsNotExist(err) {
		err := os.Mkdir(modelFolder, os.ModePerm)
		if err != nil {
			log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Error creating sdfs model folder folder\n")
		}
	}

	// rename the model file to model.py
	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Moving the code file to within the directory containing representing the model")
	e := os.Rename(
		fmt.Sprintf("%v/%v.py", conf.SDFSModelsFolder, modelId),
		fmt.Sprintf("%v/model.py", modelFolder),
	)
	if e != nil {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Moving the model code file failed - %v", e)
		// return &ws.SetupModelReply{
		// 	Status: false,
		// }, errors.New("Moving the model code failed")
	}

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Moving the weights file to within the directory containing representing the model")
	e = os.Rename(
		fmt.Sprintf("%v/%v.weights.h5", conf.SDFSModelsFolder, modelId),
		fmt.Sprintf("%v/model.weights.h5", modelFolder),
	)
	if e != nil {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Moving the model weights file failed - %v", e)
		// return &ws.SetupModelReply{
		// 	Status: false,
		// }, errors.New("Moving the model weights failed")
	}

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Moved the model to folder %v", modelFolder)

	// generate wrapper function

	wrapper := `
import socket
from model import Model
import sys

if __name__ == "__main__":
	# SOCK_DGRAM -> UDP
	# SOCK_STREAM -> TCP
	HOSTNAME = socket.gethostbyname( '0.0.0.0' )
	PORT_NUMBER = int(sys.argv[1])
	SIZE = 4096
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_socket.bind((HOSTNAME, PORT_NUMBER))

	print(f"Server address: {HOSTNAME}")
	print(f"Server listening on port {PORT_NUMBER}")

	# queue of 5 just in case
	server_socket.listen(5)

	flag = True

	while flag:
		# create a new socket instance to handle the connection
		new_socket, address = server_socket.accept()

		# receive
		byte_message = new_socket.recv(SIZE)
		message = byte_message.decode("utf-8")
		print(f"Received packet from {address}: {message}")

		# send
		reply = Model("Shahid").process()
		new_socket.send(bytes(reply, "utf-8"))
		print(f"Sent packet to {address}: {reply}")
		new_socket.close()
`
	wrapperfilepath := fmt.Sprintf("%v/model-wrapper.py", modelFolder)
	f, err := os.Create(wrapperfilepath)

	if err != nil {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Error while forming the wrapper code for the model")
		return &ws.SetupModelReply{
			Status: false,
		}, errors.New("Fetching of model code failed")
	}
	w := bufio.NewWriter(f)
	n4, _ := w.WriteString(wrapper)

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Generating wrapper.py: Wrote %d bytes\n", n4)

	w.Flush()

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Cchanging the permissions of the wrapper file to allow execute")
	err = os.Chmod(wrapperfilepath, 0777)
	if err != nil {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Error while changing the permissions of the wrapper file to allow execute")
		// return &ws.SetupModelReply{
		// 	Status: false,
		// }, errors.New("Error while changing the permissions of the wrapper file to allow execute")
	}

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Changing the permissions of the model file to allow execute")
	err = os.Chmod(fmt.Sprintf("%v/model.py", modelFolder), 0777)
	if err != nil {
		log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Error while changing the permissions of the model file to allow execute")
		// return &ws.SetupModelReply{
		// 	Status: false,
		// }, errors.New("Error while changing the permissions of the wrapper file to allow execute")
	}

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Running the model")

	workerport := workerState.RecordModel(modelId)

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ] Started model %v on port %v", modelId, workerport)

	grepCommand := fmt.Sprintf("python3 %v %v &", wrapperfilepath, workerport)

	log.Printf("[ Worker ][ DeployModel ][ SetupModel ]Executing: %v", grepCommand)

	// Exectute the underlying os grep command for the given
	exec.Command("bash", "-c", grepCommand).Run()

	// Start a thread that will poll the scheduler for queries whenever it is not executing one
	log.Printf("[ Worker ][ Inferencing ]Initialising Polling of scheduler for queries")
	go pollSchedulerForQueries(modelId)

	return &ws.SetupModelReply{
		Status: true,
	}, nil
}

func pollSchedulerForQueries(modelId string) {
	conf := config.GetConfig("../../config/config.json")
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	for {
		log.Printf("[ Worker ][ Inferencing ][ pollSchedulerForQueries ]Polling scheduler")
		r, err := client.GimmeQuery(ctx, &ss.GimmeQueryRequest{
			ModelId:  modelId,
			WorkerId: workerState.GetId(),
		})

		if err != nil {
			log.Printf("[ Worker ][ Inferencing ][ pollSchedulerForQueries ]Issue connecting to the scheduler")
		}

		if !r.GetStatus() {
			log.Printf("[ Worker ][ Inferencing ][ pollSchedulerForQueries ]No queries are there to process yet")
		} else {
			queryStatus, resultfilename := processQuery(r.GetQueryinputfile())

			log.Printf("[ Worker ][ Inferencing ][ pollSchedulerForQueries ]Status of the query processing: %v; Result of the query stored in the filename: %v", queryStatus, resultfilename)

			go informSchedulerOfQueryExecution(modelId, resultfilename)
		}

		time.Sleep(time.Duration(conf.SchedulerPollInterval) * time.Second)
	}
}

func processQuery(testfilename string) (bool, string) {
	// get the testfile from sdfs

	// query the model that is running

	// save the result file on sdfs

	// return the status of the processing and filename

	return true, "filename"
}

func informSchedulerOfQueryExecution(modelId string, filename string) {
	// ack the scheduler to let it know about the deployment
}
