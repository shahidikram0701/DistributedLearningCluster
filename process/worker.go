package process

import (
	"bufio"
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	ws "cs425/mp/proto/worker_proto"
	"errors"
	"fmt"
	"io/ioutil"
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

	weightFile := fmt.Sprintf("%v.weights.pth", modelId)
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
		fmt.Sprintf("%v/%v.weights.pth", conf.SDFSModelsFolder, modelId),
		fmt.Sprintf("%v/model.weights.pth", modelFolder),
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
import os
import logging

logging.basicConfig(
	filename="model-wrapper.log",
	filemode='a',
	format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
	datefmt='%H:%M:%S',
	level=logging.DEBUG
)

# SOCK_DGRAM -> UDP
# SOCK_STREAM -> TCP
HOSTNAME = socket.gethostbyname( '0.0.0.0' )
PORT_NUMBER = int(sys.argv[1])
SIZE = 4096
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOSTNAME, PORT_NUMBER))

logging.info(f"Server address: {HOSTNAME}")
logging.info(f"Server listening on port {PORT_NUMBER}")

# queue of 5 just in case
server_socket.listen(5)

model = Model()

flag = True

while flag:
	# create a new socket instance to handle the connection
	new_socket, address = server_socket.accept()

	# receive
	byte_message = new_socket.recv(SIZE)
	inputfilepath = byte_message.decode("utf-8")
	logging.info(f"Received packet from {address}: {inputfilepath}")

	# send
	label = model.process(inputfilepath)
	isExist = os.path.exists("../../data/")
	if not isExist:
		os.mkdir("../../data/")
		
	outfile = inputfilepath.split("/")[-1] + "-out"
	with open("../../data/" + outfile, 'w') as f:
		f.write(label)
		
	new_socket.send(bytes(outfile, "utf-8"))
	logging.info(f"Sent packet to {address}: {outfile}")
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
	err = exec.Command("bash", "-c", grepCommand).Run()

	if err != nil {
		log.Fatalf("[ Worker ][ DeployModel ][ SetupModel ]Error running the model; error: %v", err)
		return &ws.SetupModelReply{
			Status: false,
		}, nil
	}

	// Start a thread that will poll the scheduler for queries whenever it is not executing one
	log.Printf("[ Worker ][ Inferencing ]Initialising Polling of scheduler for queries")
	go pollSchedulerForQueries(modelId)

	return &ws.SetupModelReply{
		Status: true,
	}, nil
}

func pollSchedulerForQueries(modelId string) {
	workerId := workerState.GetId()
	for {
		conf := config.GetConfig("../../config/config.json")
		client, ctx, conn, cancel := getClientForSchedulerService()

		log.Printf("[ Worker ][ ModelInference ][ pollSchedulerForQueries ]Polling scheduler for task of model: %v by worker: %v", modelId, workerId)
		r, err := client.GimmeQuery(ctx, &ss.GimmeQueryRequest{
			ModelId:  modelId,
			WorkerId: workerId,
		})

		if err != nil {
			log.Printf("[ Worker ][ ModelInference ][ pollSchedulerForQueries ]Issue connecting to the scheduler: %v", err)
		} else if !r.GetStatus() {
			log.Printf("[ Worker ][ ModelInference ][ pollSchedulerForQueries ]No queries are there to process yet")
		} else {
			log.Printf("[ Worker ][ ModelInference ][ pollSchedulerForQueries ]Query to execute: model:%v:::Task:%v:::Input:%v", modelId, r.GetTaskId(), r.GetQueryinputfile())
			resultfilename, queryStatus := processQuery(modelId, r.GetTaskId(), r.GetQueryinputfile())

			log.Printf("[ Worker ][ ModelInference ][ pollSchedulerForQueries ]Status of the query processing: %v; Result of the query stored in the filename: %v", queryStatus, resultfilename)

			go informSchedulerOfQueryExecution(modelId, r.GetTaskId(), resultfilename)

			conn.Close()
			cancel()
			continue
		}
		conn.Close()
		cancel()

		time.Sleep(time.Duration(conf.SchedulerPollInterval) * time.Second)
	}
}

func processQuery(modelId string, taskId string, queryinputfile string) (string, bool) {
	// check if the query input file is already present at the worker
	conf := config.GetConfig("../../config/config.json")

	queryFileExists := false

	files, err := ioutil.ReadDir(conf.OutputDataFolder)
	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ processQuery ]Query File doesnt already exist. Need to fetch")
	}

	for _, file := range files {
		if file.Name()[0] == '.' {
			continue
		}
		if file.Name() == queryinputfile {
			queryFileExists = true
			break
		}
	}

	if !queryFileExists {
		// get the testfile from sdfs
		log.Printf("[ Worker ][ ModelInference ][ processQuery ]Getting the query file form SDFS")
		getFileStatus := GetFile(queryinputfile, queryinputfile, false)

		if !getFileStatus {
			log.Printf("[ Worker ][ ModelInference ][ processQuery ]Couldn't fetch the query input file: %v", queryinputfile)
			return "", false
		} else {
			log.Printf("[ Worker ][ ModelInference ][ processQuery ]Successfully fetched the query file")
		}
	}
	// query the model that is running
	port, ok := workerState.GetModelPort(modelId)

	if !ok {
		log.Printf("[ Worker ][ ModelInference ][ processQuery ]Model %v not executing on this worker", modelId)

		return "", false
	}

	outfile, ok := queryTheModel(port, queryinputfile)

	if !ok {
		log.Printf("[ Worker ][ ModelInference ][ processQuery ]Model Query FAILED")
		return "", false
	}

	log.Printf("[ Worker ][ ModelInference ][ process Query ]Saving the outfile(%v) to SDFS: ", outfile)
	// save the result file on sdfs
	if !PutFile(outfile, outfile) {
		log.Printf("[ Worker ][ ModelInference ][ processQuery ]Error saving the output file: %v to SDFS", outfile)

		return "", false
	}
	log.Printf("[ Worker ][ ModelInference ][ processQuery ]Successfully saved the output file: %v to SDFS", outfile)

	return outfile, true
}

func queryTheModel(port int, queryinputfile string) (string, bool) {
	conf := config.GetConfig("../../config/config.json")
	hostname := dataNode_GetOutboundIP().String()
	addrs, err := net.LookupHost(hostname)

	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Error Looking up hostname: %v", err)
		return "", false
	}

	log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Service address: %v:%v", addrs, port)

	service := fmt.Sprintf("%v:%v", addrs[0], port)

	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Error Resolving TCP Addr: %v", err)
		return "", false
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Error Dialing TCP: %v", err)
		return "", false
	}

	queryinputfilepath := fmt.Sprintf("%v/%v/%v-1", conf.OutputDataFolder, queryinputfile, queryinputfile)

	log.Printf("[ Worker ][ InferenceModel ]Query Input File Path: %v", queryinputfilepath)
	_, err = conn.Write([]byte(queryinputfilepath))

	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Error Writing the query input file name to the buffer to send over to the model: %v", err)
		return "", false
	}

	outfile, err := ioutil.ReadAll(conn)
	if err != nil {
		log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Error Reading data from the model: %v", err)
		return "", false
	}
	log.Printf("[ Worker ][ ModelInference ][ queryTheModel ]Successfully Queried the model and generated the outfile: %v", outfile)
	return string(outfile), true
}

func informSchedulerOfQueryExecution(modelId string, taskId string, filename string) {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	_, err := client.UpdateQueryStatus(ctx, &ss.UpdateQueryStatusRequest{
		TaskId:         taskId,
		Outputfilename: filename,
	})

	if err != nil {
		log.Printf("[ Scheduler ][ ModelInference ][ informSchedulerOfQueryExecution ]Informing scheduler of task(%v) completion Failed: %v", taskId, err)
	} else {
		log.Printf("[ Scheduler ][ ModelInference ][ informSchedulerOfQueryExecution ]Successfully Informed scheduler of the completion of task %v", taskId)
	}
}
