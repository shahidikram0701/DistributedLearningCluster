package process

import (
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	ws "cs425/mp/proto/worker_proto"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	return c, ctx, conn, cancel
}

func getClientForWorkerService(workerIp string) (ws.WorkerServiceClient, context.Context, *grpc.ClientConn, context.CancelFunc) {
	conf := config.GetConfig("../../config/config.json")
	workerPort := conf.WorkerPort
	replica := fmt.Sprintf("%v:%v", workerIp, workerPort)

	log.Printf("[ getClientForWorkerService ]Getting the grpc client for the worker: %v", replica)

	conn, err := grpc.Dial(replica, grpc.WithInsecure())
	if err != nil {
		log.Printf("[ getClientForWorkerService ]Failed to establish connection with the worker....Retrying")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	// Initialise a client to connect to the coordinator process
	client := ws.NewWorkerServiceClient(conn)

	return client, ctx, conn, cancel
}

func DeployModel(modelname string) bool {
	conf := config.GetConfig("../../config/config.json")
	weightsfilename := modelname + ".weights.h5"
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
		log.Printf("[ Client ][ DeployModel ][ DeployModel ]Failed to establish connection with the scheduler service: %v", err)

		return false
	}

	log.Printf("[ Client ][ DeployModel ][ DeployModel ]Model Id: %v; Workers: %v", r.GetModelId(), r.GetWorkers())

	// save the weights to SDFS
	log.Printf("[ Client ][ DeployModel ][ DeployModel ]Saving the weights to SDFS")

	sdfsModelWeightsFilename := fmt.Sprintf("%v.weights.h5", r.GetModelId())
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

	return deploymentStatus

}

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

func setupModelOnWorker(worker string, modelId string, responseChannel chan bool) {
	client, ctx, conn, cancel := getClientForWorkerService(worker)

	defer conn.Close()
	defer cancel()

	log.Printf("[ Client ][ DeployModel ][ setupModelOnWorker ]Setting up the model on the client: %v", worker)

	r, err := client.SetupModel(ctx, &ws.SetupModelRequest{
		ModelId: modelId,
	})

	if err != nil {
		log.Printf("[ Client ][ DeployModel ][ setupModelOnWorker ]Error setting up the model: %v", err)

		responseChannel <- false
	}

	responseChannel <- r.GetStatus()
}
