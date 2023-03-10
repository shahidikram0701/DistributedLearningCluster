package process

import (
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	ws "cs425/mp/proto/worker_proto"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/**
* Batchsize of the tasks
 */
var (
	BatchSize map[string]int
)

/**
* Get rpc client to the scheduler service
 */
func getClientForSchedulerService() (ss.SchedulerServiceClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")
	schedulerAddr := fmt.Sprintf("%v:%v", memberList.GetCoordinatorNode(), conf.SchedulerPort)

	log.Printf("[ Utility ][ getClientForSchedulerService ]Getting the grpc cliend for the Scheduler at: %v", schedulerAddr)
	conn, err := grpc.Dial(schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("[ Utility ][ getClientForSchedulerService ]Failed to establish connection with the coordinator: %v", err)
	}

	// Initialise a client to connect to the coordinator process
	c := ss.NewSchedulerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	return c, ctx, conn, cancel
}

/*
* Get rpc client to the worker service
 */
func getClientForWorkerService(workerIp string) (ws.WorkerServiceClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")
	workerPort := conf.WorkerPort
	replica := fmt.Sprintf("%v:%v", workerIp, workerPort)

	log.Printf("[ getClientForWorkerService ]Getting the grpc client for the worker: %v", replica)

	conn, err := grpc.Dial(replica, grpc.WithInsecure())
	if err != nil {
		log.Printf("[ getClientForWorkerService ]Failed to establish connection with the worker....Retrying")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)

	// Initialise a client to connect to the coordinator process
	client := ws.NewWorkerServiceClient(conn)

	return client, ctx, conn, cancel
}

/*
* Frontend function for the client to deploy model
 */
func DeployModel(modelname string) bool {
	conf := config.GetConfig("../../config/config.json")
	weightsfilename := modelname + ".weights.pth"
	codefile := modelname + ".py"

	basepath := fmt.Sprintf("%v/%v", conf.ModelsDataFolder, modelname)

	if _, err := os.Stat(basepath); os.IsNotExist(err) {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Input model isnt defined")
		return false
	}

	if _, err := os.Stat(fmt.Sprintf("%v/%v", basepath, weightsfilename)); os.IsNotExist(err) {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Weights arent defined")
		return false
	}

	if _, err := os.Stat(fmt.Sprintf("%v/%v", basepath, codefile)); os.IsNotExist(err) {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Code file isnt defined")
		return false
	}

	log.Printf("[ Client ][ DeployModel ][ DeployModel ]Deploying model: %v whose weight file is %v", modelname, weightsfilename)

	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.DeployModel(ctx, &ss.DeployModelRequest{Modelname: modelname})

	if err != nil {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Deploy model failed: %v", err)

		return false
	}

	log.Printf("[ Client ][ DeployModel ][ DeployModel ]Model Id: %v; Workers: %v", r.GetModelId(), r.GetWorkers())

	// save the weights to SDFS
	log.Printf("[ Client ][ DeployModel ][ DeployModel ]Saving the weights to SDFS")

	sdfsModelWeightsFilename := fmt.Sprintf("%v.weights.pth", r.GetModelId())
	sdfsModelCodeFilename := fmt.Sprintf("%v.py", r.GetModelId())
	if !PutFile(sdfsModelWeightsFilename, weightsfilename, basepath) {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Error saving weights of the model")
		return false
	}
	log.Printf("[ Client ][ DeployModel ]Saved the weights of the model to SDFS")

	if !PutFile(sdfsModelCodeFilename, codefile, basepath) {
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Error uploading the code file")
		return false
	}
	log.Printf("[ Client ][ DeployModel ]Saved the code of the model to SDFS")

	deploymentStatus := deployModelOnWorkers(r, conf.NumOfWorkersPerModel)

	log.Printf("[ Client ][ DeployModel ]Deployment Status: %v", deploymentStatus)

	// Inform the scheduler of the deployment status
	log.Printf("[ Client ][ DeployModel ]Inform master of the deployment status: %v", deploymentStatus)

	client.DeployModelAck(ctx, &ss.DeployModelAckRequest{
		ModelId: r.GetModelId(),
		Status:  deploymentStatus,
	})

	if deploymentStatus {
		BatchSize[modelname] = conf.InitialBatchSize
	}

	return deploymentStatus
}

/*
* Deploy model on all the workers
 */
func deployModelOnWorkers(r *ss.DeployModelReply, numWorkers int) bool {

	modelId := r.GetModelId()
	workers := r.GetWorkers()

	responseChannel := make(chan bool)

	for _, worker := range workers {
		go setupModelOnWorker(worker, modelId, responseChannel)
	}

	for i := 0; i < numWorkers; i++ {
		status := <-responseChannel
		if !status {
			log.Printf("[ Client ][ DeployModel ][ deployModelOnWorkers ]Setting up model failed on a worker")
			return false
		}
	}

	log.Printf("[ Client ][ DeployModel ][ deployModelOnWorkers ]Set up models on all the worker nodes")

	return true
}

/*
* Utility function to deploy model on one worker
 */
func setupModelOnWorker(worker string, modelId string, responseChannel chan bool) {
	client, ctx, conn, cancel := getClientForWorkerService(worker)

	defer conn.Close()
	defer cancel()

	log.Printf("[ Client ][ DeployModel ][ setupModelOnWorker ]Setting up the model on the worker: %v", worker)

	r, err := client.SetupModel(ctx, &ws.SetupModelRequest{
		ModelId: modelId,
	})

	if err != nil {
		log.Printf("[ Client ][ DeployModel ][ setupModelOnWorker ]Error setting up the model: %v", err)

		responseChannel <- false
	}

	responseChannel <- r.GetStatus()
}

/*
* Front end function for the client to query a certain model
 */
func QueryModel(modelname string, queryinputfilenames []string) string {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.SubmitTask(ctx, &ss.SubmitTaskRequest{
		Modelname:       modelname,
		Queryinputfiles: queryinputfilenames,
		Owner:           dataNode_GetOutboundIP().String(),
		Creationtime:    time.Now().String(),
	})

	if err != nil {
		log.Printf("[ Client ][ ModelInference ][ QueryModel ]Submit task failed: %v", err)
		return fmt.Sprintf("Submit Task failed: %v", err)
	}

	return fmt.Sprintf("Submitted Task: %v", r.GetTaskId())
}

/*
* Frontend function to fetch all the tasks of a user
 */
func GetAllTasks() []Task {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.GetAllTasks(ctx, &ss.GetAllTasksRequest{
		Owner: dataNode_GetOutboundIP().String(),
	})

	if err != nil {
		log.Printf("[ Client ][ GetAllTasks ]GetAllTasks failed: %v", err)
		return []Task{}
	}

	var tasks []Task
	unmarshallingError := json.Unmarshal(r.Tasks, &tasks)
	if unmarshallingError != nil {
		log.Printf("[ Client ][ GetAllTasks ]Error while unmarshalling the tasks: %v\n", unmarshallingError)
		return []Task{}
	}

	return tasks
}

/*
* Fetch All the tasks of the user specific to a model
 */
func GetAllTasksOfModel(modelname string) []Task {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.GetAllTasksOfModel(ctx, &ss.GetAllTasksOfModelRequest{
		Owner:     dataNode_GetOutboundIP().String(),
		Modelname: modelname,
	})

	if err != nil {
		log.Printf("[ Client ][ GetAllTasksOfModel ]GetAllTasksOfModel failed: %v", err)
		return []Task{}
	}

	var tasks []Task
	unmarshallingError := json.Unmarshal(r.Tasks, &tasks)
	if unmarshallingError != nil {
		log.Printf("[ Client ][ GetAllTasksOfModel ]Error while unmarshalling the tasks: %v\n", unmarshallingError)
		return []Task{}
	}

	return tasks
}

/*
* Start batch inferencing queries
 */
func StartInference(modelName string, filenames []string) {
	go runInferencePeriodically(modelName, filenames)
}

/*
* gofunc to run inference periodically
 */
func runInferencePeriodically(modelName string, filenames []string) {
	conf := config.GetConfig("../../config/config.json")
	i := 0

	for numTasks := 0; numTasks < 100; numTasks++ {
		batch := []string{}
		_, ok := BatchSize[modelName]
		if !ok {
			BatchSize[modelName] = conf.InitialBatchSize
		}
		batch_size := BatchSize[modelName]
		log.Printf("[ Client ][ ModelInference ]Task number: %v; BatchSize: %v", numTasks, batch_size)
		for b := 0; b < batch_size; b++ {
			batch = append(batch, filenames[i])
			i = (i + 1) % len(filenames)
		}

		taskId := QueryModel(modelName, batch)
		log.Printf("[ Client ][ ModelInference ]Query model %v with inputs %v; TaskId: %v", modelName, batch, taskId)

		time.Sleep(3 * time.Second)
	}
}

/*
* Fetches query rates of all the models currently deployed in the system
 */
func GetAllQueryRates() ([]string, []float32) {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()
	r, err := client.GetAllQueryRates(ctx, &ss.GetAllQueryRatesRequest{})

	if err != nil {
		log.Printf("[ Client ][ GetAllTasksOfModel ]GetAllTasksOfModel failed: %v", err)
		return []string{}, []float32{}
	}

	return r.GetModelnames(), r.GetQueryrates()
}

/*
* Set the batch size of the tasks for a model
 */
func SetBatchSize(modelname string, batchsize int) int {
	BatchSize[modelname] = batchsize
	return BatchSize[modelname]
}

/*
* Get the batchsize of tasks on a model
 */
func GetBatchSize(modelname string) int {
	return BatchSize[modelname]
}

/*
* Get the number of queries executed on a model
 */
func GetQueryCount() ([]string, []int32) {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.GetQueryCount(ctx, &ss.GetQueryCountRequest{})

	if err != nil {
		log.Printf("[ Client ][ GetQueryCount ]Error: %v", err)
		return []string{}, []int32{}
	}

	return r.GetModelnames(), r.GetQuerycount()
}

/*
* Fetch all the workers of the model
 */
func GetAllWorkersOfModel(modelname string) []string {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.GetWorkersOfModel(ctx, &ss.GetWorkersOfModelRequest{
		Modelname: modelname,
	})

	if err != nil {
		log.Printf("[ Client ][ GetQueryCount ]Error: %v", err)
		return []string{}
	}

	return r.GetWorkers()
}

/*
* Get the average exec times queries executed on a model
 */
func GetAvgExecTimes() ([]string, []float32) {
	client, ctx, conn, cancel := getClientForSchedulerService()
	defer conn.Close()
	defer cancel()

	r, err := client.GetQueryAverageExectionTimes(ctx, &ss.GetQueryAverageExectionTimeRequest{})

	if err != nil {
		log.Printf("[ Client ][ GetAvgExecTimes ]Error: %v", err)
		return []string{}, []float32{}
	}

	return r.GetModelnames(), r.GetExectimes()
}
