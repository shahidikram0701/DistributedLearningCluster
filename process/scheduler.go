package process

import (
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	ws "cs425/mp/proto/worker_proto"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
* RPC Scheduler Service
 */
type SchedulerServer struct {
	ss.UnimplementedSchedulerServiceServer
}

// state of the scheduler process
var (
	schedulerState *SchedulerState
)

/*
* Start the scheduler service
 */
func SchedulerService_IDunno(port int, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	ss.RegisterSchedulerServiceServer(s, &SchedulerServer{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}

type ModelStatus int

const (
	Undeployed ModelStatus = iota
	Deployed
)

func (modelStatus ModelStatus) String() string {
	switch modelStatus {
	case Undeployed:
		return "Undeployed"
	case Deployed:
		return "Deployed"
	default:
		return fmt.Sprintf("%d", int(modelStatus))
	}
}

// Type: Model
type Model struct {
	Name                 string
	Id                   string
	Workers              []string
	QueryRate            float64
	CreationTime         time.Time
	Status               ModelStatus
	QueryCount           int
	AverageQueryExecTime float64
}

// Type: Tasks of a model
type ModelTasks struct {
	lock  *sync.RWMutex
	tasks []string
}

/*
* Method to add task to a model task queue
 */
func (mt *ModelTasks) AddTaskToModel(taskId string) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	log.Printf("[ Scheduler ][ ModelTasks ]Adding Task to model")

	mt.tasks = append(mt.tasks, taskId)
}

/*
* Get the next task that has to be scheduled for a model
 */
func (mt *ModelTasks) GetTaskToSchedule(modelId string, workerId string) ([]string, string, bool) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	log.Printf("[ Scheduler ][ TaskScheduler ]Getting task to schedule for model %v at worker: %v", modelId, workerId)

	for _, taskid := range mt.tasks {
		if schedulerState.Tasks[taskid].Status == Ready {
			task := schedulerState.Tasks[taskid]

			log.Printf("[ Scheduler ][ ModelInference ][ GetTaskToSchedule ]Next task to be allocated for model %v::%v is %v", modelId, task.ModelName, taskid)

			task.AssignedTo = workerId
			task.StartTime = time.Now()
			task.Status = Waiting

			log.Printf("[ Scheduler ][ Waittime ]Wait time for %v task: %v", task.ModelName, time.Since(task.ArrivalTime))

			// Start execution timeout timer for this task
			go handleTaskExecutionTimeout(taskid, modelId, workerId)

			// Appending the task to the window of tasks
			schedulerState.WindowOfTasks = append(schedulerState.WindowOfTasks, taskid)

			schedulerState.Tasks[taskid] = task

			return task.Filenames, taskid, true
		}
	}

	return []string{}, "", false
}

func (mt ModelTasks) String() string {
	return fmt.Sprintf("Tasks: %v", mt.tasks)
}

func (model Model) String() string {
	return fmt.Sprintf("(ModelName, %v), (ModelId, %v), (Workers, %v), (CreationTime, %v), (Status, %v)", model.Name, model.Id, model.Workers, model.CreationTime, model.Status)
}

/*
* Method to increment the query counter of model on successful task completion
 */
func (state *SchedulerState) IncrementQueryCount(modelId string, processedQueries int) {
	state.lock.Lock()
	defer state.lock.Unlock()

	model := state.Models[modelId]
	model.QueryCount += processedQueries

	state.Models[modelId] = model

	log.Printf("[ Scheduler ][ QueryCount ]Updating query count of model: %v by %v to finally become %v", model.Name, processedQueries, model.QueryCount)
}

/*
* Method to get the query counts of all the models deployed in the system
 */
func (state *SchedulerState) GetQueryCounts() ([]string, []int32) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	modelnames := []string{}
	querycounts := []int32{}

	for modelId := range state.Models {
		modelnames = append(modelnames, state.Models[modelId].Name)
		querycounts = append(querycounts, int32(state.Models[modelId].QueryCount))
	}

	return modelnames, querycounts
}

/*
* Method to get the exec times of all the models deployed in the system
 */
func (state *SchedulerState) GetExectimes() ([]string, []float32) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	modelnames := []string{}
	exectimes := []float32{}

	for modelId := range state.Models {
		modelnames = append(modelnames, state.Models[modelId].Name)
		exectimes = append(exectimes, float32(state.Models[modelId].AverageQueryExecTime))
	}

	return modelnames, exectimes
}

// Type: Task
type Task struct {
	Id              string
	Name            string
	Status          TaskStatus
	CreationTime    string
	AssignedTo      string
	ModelId         string
	ModelName       string
	StartTime       time.Time
	EndTime         time.Time
	OwnerId         string
	Filenames       []string // input filename
	Resultfilenames []string
	ArrivalTime     time.Time
}

type TaskStatus int

const (
	Ready TaskStatus = iota
	Waiting
	Success
	Fail
)

func (taskStatus TaskStatus) String() string {
	switch taskStatus {
	case Ready:
		return "Ready"
	case Waiting:
		return "Waiting"
	case Success:
		return "Success"
	case Fail:
		return "Fail"
	default:
		return fmt.Sprintf("%d", int(taskStatus))
	}
}

func (task Task) String() string {
	return fmt.Sprintf(
		"\tId: %v\n\tStatus: %v\n\tCreated At: %v\n\tModel: %v\n\tInput File: %v\n\tOutput File: %v\n",
		task.Id, task.Status, task.CreationTime, task.ModelName, task.Filenames, task.Resultfilenames,
	)
}

// Type: State of the Scheduler process
type SchedulerState struct {
	lock                *sync.RWMutex
	Models              map[string]Model
	Tasks               map[string]Task        // all the tasks in the system
	TaskQueue           map[string]*ModelTasks //model-id to task-id mapping
	WindowOfTasks       []string
	IndexIntoMemberList int
	ModelNameToId       map[string]string
	taskTimer           map[string]*time.Timer
	queryRateDropTime   time.Time
}

/*
* Method to get the model name
 */
func (state *SchedulerState) GetModelName(modelId string) string {
	state.lock.RLock()
	defer state.lock.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelName ]Invalid Model Id")
		return ""
	}

	return state.Models[modelId].Name
}

/*
* Method to fetch the Id of a model given model name
 */
func (state *SchedulerState) GetModelId(modelname string) (string, bool) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	modelId, ok := state.ModelNameToId[modelname]

	return modelId, ok
}

/*
* Method to fetch all the workers of a model
 */
func (state *SchedulerState) GetWorkersOfModel(modelId string) []string {
	state.lock.RLock()
	defer state.lock.RUnlock()

	workers := []string{}

	for _, worker := range state.Models[modelId].Workers {
		if memberList.IsNodeAlive(worker) {
			workers = append(workers, worker)
		}
	}

	return workers

}

/*
* Method to get all the tasks of a particular client
 */
func (state *SchedulerState) GetAllTasksOfClient(clientId string) []Task {
	state.lock.RLock()
	defer state.lock.RUnlock()

	tasks := []Task{}

	for _, task := range state.Tasks {
		if task.OwnerId == clientId {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

/*
* Method to get all the tasks of a desired model pertaining to a client
 */
func (state *SchedulerState) GetAllTasksOfModelOfClient(modelname string, clientId string) []Task {
	state.lock.RLock()
	defer state.lock.RUnlock()

	tasks := []Task{}

	for _, task := range state.Tasks {
		if task.OwnerId == clientId && task.ModelName == modelname {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

/*
* Method to get the query rate of a model
 */
func (state *SchedulerState) GetQueryRateOfModel(modelId string) float64 {
	state.lock.RLock()
	defer state.lock.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetQueryRateOfModel ]Invalid Model Id")
		return -1
	}

	return state.Models[modelId].QueryRate
}

/*
* Method to add a model to scheduler state on deployment
 */
func (state *SchedulerState) AddModel(modelname string) (string, []string) {
	state.lock.Lock()
	defer state.lock.Unlock()
	conf := config.GetConfig("../../config/config.json")

	modelId := uuid.New().String()
	log.Printf("[ Scheduler ][ DeployModel ][ AddModel ]New modelId for model %v: %v", modelname, modelId)

	nodes, newIndexIntoMemberlist := memberList.GetNDataNodes(state.IndexIntoMemberList, conf.NumOfWorkersPerModel)
	state.IndexIntoMemberList = newIndexIntoMemberlist

	log.Printf("[ Scheduler ][ DeployModel ][ AddModel ]Allocated workers for model %v with modelId %v: %v", modelname, modelId, nodes)

	model := Model{
		Name:         modelname,
		Id:           modelId,
		QueryRate:    0.0,
		CreationTime: time.Now(),
		Workers:      nodes,
		Status:       Undeployed,
	}

	state.Models[modelId] = model
	state.ModelNameToId[modelname] = modelId
	state.TaskQueue[modelId] = &ModelTasks{
		lock:  &sync.RWMutex{},
		tasks: []string{},
	}

	return modelId, nodes
}

/*
* Method to queue an incomning task from the client for a particular model
 */
func (state *SchedulerState) QueueTask(modelname string, modelId string, queryinputfiles []string, owner string, creationTime string) string {
	// state.lock.Lock()
	// defer state.lock.Unlock()

	taskId := uuid.New().String()
	log.Printf("[ Scheduler ][ ModelInference ][ QueueTask ]New taskId for model %v::%v: %v", modelname, modelId, taskId)

	task := Task{
		Id:           taskId,
		Status:       Ready,
		CreationTime: creationTime,
		ModelId:      modelId,
		ModelName:    modelname,
		OwnerId:      owner,
		Filenames:    queryinputfiles,
		ArrivalTime:  time.Now(),
	}

	log.Printf("[ Scheduler ][ ModelInference ]Adding task: %v to queue of model %v::%v", task, modelname, modelId)

	state.Tasks[taskId] = task

	state.lock.Lock()
	if _, ok := state.TaskQueue[modelId]; !ok {
		state.TaskQueue[modelId] = &ModelTasks{
			lock:  &sync.RWMutex{},
			tasks: []string{},
		}
	}
	state.lock.Unlock()

	state.TaskQueue[modelId].AddTaskToModel(taskId)

	return taskId
}

/*
* Method to mark the model as deployed on notification from the client
 */
func (state *SchedulerState) MarkModelAsDeployed(modelId string) {
	state.lock.Lock()
	defer state.lock.Unlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Invalid Model Id")
		return
	}
	log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Model %v deployed", modelId)
	model := state.Models[modelId]
	model.Status = Deployed

	state.Models[modelId] = model
}

/*
* Method to delete a model
 */
func (state *SchedulerState) RemoveModel(modelId string) {
	state.lock.Lock()
	defer state.lock.Unlock()

	log.Printf("[]")

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ RemoveModel ]Invalid Model Id")

		return
	}
	log.Printf("[ Scheduler ][ RemoveModel ]Removed the model %v from the set of active models", modelId)
	delete(state.Models, modelId)
}

/*
* Method to get the status of the model deployment
 */
func (state *SchedulerState) GetModelStatus(modelId string) (ModelStatus, bool) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelStatus ]Invalid Model Id")
		return -1, false
	}

	return state.Models[modelId].Status, true
}

/*
* Method for scheduling the next task on request from the worker
 */
func (state *SchedulerState) GetNextTaskToBeScheduled(modelId string, workerId string) ([]string, string, bool) {
	// state.lock.Lock()
	// defer state.lock.Unlock()

	log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]Model: %v; Worker: %v", modelId, workerId)
	tasks := state.TaskQueue[modelId]
	log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]TaskQueue[%v]: %v", modelId, tasks)

	if _, ok := state.TaskQueue[modelId]; !ok {
		return []string{}, "", false
	}

	return state.TaskQueue[modelId].GetTaskToSchedule(modelId, workerId)

	// for _, taskid := range tasksofModel {
	// 	if state.Tasks[taskid].Status == Ready {
	// 		task := state.Tasks[taskid]
	// 		log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]Next task to be allocated for model %v::%v is %v", modelId, task.ModelName, taskid)
	// 		task.AssignedTo = workerId
	// 		task.StartTime = time.Now()
	// 		task.Status = Waiting

	// 		// Start execution timeout timer for this task
	// 		go handleTaskExecutionTimeout(taskid, modelId, workerId)

	// 		// Appending the task to the window of tasks
	// 		state.WindowOfTasks = append(state.WindowOfTasks, taskid)

	// 		state.Tasks[taskid] = task

	// 		return task.Filenames, taskid, true
	// 	}
	// }

	// return []string{}, "", false
}

/*
* Method to handle reschedule of the tasks in case wait time exceeeded or worker failed
 */
func (state *SchedulerState) HandleRescheduleOfTask(taskId string) {
	state.lock.Lock()
	defer state.lock.Unlock()

	task := state.Tasks[taskId]
	task.AssignedTo = ""
	task.StartTime = time.Time{}
	task.Status = Ready

	state.Tasks[taskId] = task

	log.Printf("[ Scheduler ][ ModelInference ][ HandleRescheduleOfTask ]Updated state of the task %v to Ready", taskId)
}

/*
* Method to mark the task as complete
 */
func (state *SchedulerState) MarkTaskComplete(taskId string, outputFiles []string) {
	state.lock.Lock()
	defer state.lock.Unlock()

	task := state.Tasks[taskId]

	task.Status = Success
	task.EndTime = time.Now()
	task.Resultfilenames = outputFiles

	state.Tasks[taskId] = task

	log.Printf("[ Scheduler ][ ModelInference ][ MarkTaskComplete ]Marked task %v of model %v as complete; Output file: %v", taskId, task.ModelName, outputFiles)

	model := state.Models[task.ModelId]
	numQueries := len(task.Filenames)

	log.Printf("[ Scheduler ][ ModelInference ][ MarkTaskComplete ]Incrementing the number of queries processed by model %v by %v", model.Name, numQueries)

	model.QueryCount += numQueries

	state.Models[task.ModelId] = model
}

/*
* Method to update the query rates of all the models deployed over a past window of tasks
 */
func (state *SchedulerState) UpdateModelsQueryRates() {
	state.lock.Lock()
	defer state.lock.Unlock()

	if len(state.WindowOfTasks) == 0 {
		return
	}

	perModelCompletedCounts := map[string]int{}
	perModelTotal := map[string]int{}
	perModelExecutionTimes := map[string]float64{}

	for _, taskId := range state.WindowOfTasks {
		task := state.Tasks[taskId]

		if _, ok := perModelTotal[task.ModelId]; !ok {
			perModelTotal[task.ModelId] = 0
			perModelCompletedCounts[task.ModelId] = 0
			perModelExecutionTimes[task.ModelId] = 0.0
		}

		perModelTotal[task.ModelId] += len(task.Filenames)

		if task.Status == Success {
			perModelCompletedCounts[task.ModelId] += len(task.Filenames)
			perModelExecutionTimes[task.ModelId] += task.EndTime.Sub(task.ArrivalTime).Seconds()
		}
	}

	log.Printf("[ Scheduler ][ UpdateModelsQueryRates ]Completed Tasks: %v", perModelCompletedCounts)
	log.Printf("[ Scheduler ][ UpdateModelsQueryRates ]Total Tasks: %v", perModelTotal)

	// Clear the window of tasks
	log.Printf("[ Scheduler ][ UpdateModelsQueryRates ]Clearing the past window of tasks")
	state.WindowOfTasks = []string{}

	for modelId := range perModelTotal {
		model := state.Models[modelId]
		model.AverageQueryExecTime = perModelExecutionTimes[modelId] / float64(perModelCompletedCounts[modelId])
		log.Printf("[ Scheduler ][ Average Execution Time ]Average Execution Time of model: %v is %v", model.Name, model.AverageQueryExecTime)
		if perModelTotal[modelId] > 0 {
			model.QueryRate = float64((float64(perModelCompletedCounts[modelId]) / float64(perModelTotal[modelId])) * 100.0)
		} else {
			model.QueryRate = 0.0
		}

		log.Printf("[ Scheduler ][ UpdateModelsQueryRates ]Query Rate of model %v is %v", model.Name, model.QueryRate)

		state.Models[modelId] = model
	}

	// Check if need to deploy another model
	for modelId := range perModelTotal {
		for modelId2 := range perModelTotal {
			if modelId == modelId2 {
				continue
			}
			qr1 := state.Models[modelId].QueryRate
			qr2 := state.Models[modelId2].QueryRate

			if qr2 > qr1 && (qr2-qr1) >= 20 {
				state.queryRateDropTime = time.Now()
				log.Printf("[ Scheduler ][ Stabilisation Time ]Started time marking the query drop rate")
				// deploy a new copy of the model
				fmt.Printf("\n\t[ Scheduler ][ UpdateModelsQueryRates ]Query Rate Drop! %v: %v | %v: %v\n", state.Models[modelId].Name, state.Models[modelId].QueryRate, state.Models[modelId2].Name, state.Models[modelId2].QueryRate)

				if len(state.Models[modelId].Workers) == 9 {
					log.Printf("[ Scheduler ][ UpdateModelsQueryRates ]Model %v already has max number of workers", state.Models[modelId].Name)
					break
				}
				go addNewWorker(modelId)
				break
			} else if qr2 > qr1 {
				if !state.queryRateDropTime.IsZero() {
					log.Printf("[ Scheduler ][ Stabilisation Time ]Time since break: %v", time.Since(state.queryRateDropTime))
				}
			}

		}
	}
}

/*
* Method to add new worker for a model on scaling
 */
func (state *SchedulerState) addNewWorkerForModel(modelId string, worker string) {
	state.lock.Lock()
	defer state.lock.Unlock()

	model := state.Models[modelId]
	model.Workers = append(model.Workers, worker)

	fmt.Printf("\t[ Scheduler ][ AddNewWorker ]Added new worker %v for the model %v\n", worker, model.Name)

	state.Models[modelId] = model
}

/*
* Method for getting all the query rates across all the deployed models
 */
func (state *SchedulerState) getAllQueryRates() ([]string, []float32) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	models := []string{}
	queryRates := []float32{}

	for modelId := range state.Models {
		model := state.Models[modelId]
		models = append(models, model.Name)
		queryRates = append(queryRates, float32(model.QueryRate))
	}

	return models, queryRates
}

/*
* Method to get the snap of the scheduler state to maintain scheduler synchronisation
* for scheduller recovery
 */
func (state *SchedulerState) GetSnapOfSchedulerState() ([]byte, []byte, []byte, int, bool) {
	state.lock.RLock()
	defer state.lock.RUnlock()

	log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Sending snap of the state: %v", *state)

	serialisedModels, err := json.Marshal(state.Models)
	if err != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error Marshalling the models: %v", err)
		return nil, nil, nil, -1, false
	}
	serialisedTasks, err := json.Marshal(state.Tasks)
	if err != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error Marshalling the tasks: %v", err)
		return nil, nil, nil, -1, false
	}
	serialisedModelNameId, err := json.Marshal(state.ModelNameToId)
	if err != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error Marshalling the modelNameId: %v", err)
		return nil, nil, nil, -1, false
	}

	return serialisedModels, serialisedTasks, serialisedModelNameId, state.IndexIntoMemberList, true
}

/*
* Method to set the state of the scheduler as recieved from the main scheduler
 */
func (state *SchedulerState) SetSchedulerState(models map[string]Model, tasks map[string]Task, modelNameId map[string]string, idx int32) {
	state.lock.Lock()
	defer state.lock.Unlock()

	state.Models = models
	state.Tasks = tasks
	state.IndexIntoMemberList = int(idx)
	state.ModelNameToId = modelNameId

	log.Printf("[ Scheduler ][ Scheduler Synchornisation ]Setting scheduler state")
	newTaskQueue := make(map[string]*ModelTasks)

	for taskid := range tasks {
		task := tasks[taskid]
		if task.Status == Ready {
			if _, ok := newTaskQueue[task.ModelId]; !ok {
				newTaskQueue[task.ModelId] = &ModelTasks{
					lock:  &sync.RWMutex{},
					tasks: []string{},
				}
			}
			newTaskQueue[task.ModelId].tasks = append(newTaskQueue[task.ModelId].tasks, taskid)
		}
	}
	state.TaskQueue = newTaskQueue
	log.Printf("[ Scheduler ][ Scheduler Synchornisation ]DONE Setting scheduler state")

	// can start timer for each of the pending tasks;
}

/*
* Method to check if a worker is already assigned to a model
 */
func (state *SchedulerState) CheckIfAlreadyAWorker(modelId string, workerId string) bool {
	state.lock.RLock()
	defer state.lock.RUnlock()

	for _, worker := range state.Models[modelId].Workers {
		if worker == workerId {
			return true
		}
	}

	return false
}

/*
* Method to get all the models deployed in the system
 */
func (state *SchedulerState) GetAllModels() []string {
	state.lock.RLock()
	defer state.lock.RUnlock()

	models := []string{}

	for modelId := range state.Models {
		if state.Models[modelId].Status == Deployed {
			models = append(models, modelId)
		}
	}

	return models
}

/*
* Adds a new worker for the model when scale up necessity observed
 */
func addNewWorker(modelId string) {
	// select a new random worker
	newWorker := memberList.GetRandomNode()
	retry := 0
	for retry < 3 && schedulerState.CheckIfAlreadyAWorker(modelId, newWorker) {
		newWorker = memberList.GetRandomNode()
		retry++
	}
	if retry == 3 {
		return
	}
	modelname := schedulerState.GetModelName(modelId)
	log.Printf("[ Scheduler ][ AddNewWorker ]Adding new worker: %v for model: %v", newWorker, modelname)

	client, ctx, conn, cancel := getClientForWorkerService(newWorker)

	defer conn.Close()
	defer cancel()

	log.Printf("[ Scheduler ][ AddNewWorker ]Setting up the model on the worker: %v", newWorker)

	r, err := client.RunModel(ctx, &ws.RunModelRequest{
		ModelId: modelId,
	})
	if err != nil {
		log.Printf("[ Scheduler ][ AddNewWorker ]Error setting up the model: %v", err)
	} else if !r.GetStatus() {
		log.Printf("\t[ Scheduler ][ AddNewWorker ]Setting up of model instance %v on the new worker %v FAILED\n", modelname, newWorker)
	} else {
		schedulerState.addNewWorkerForModel(modelId, newWorker)
	}
}

/*
* Handler for ensuring the tasks are not in the Waiting state forvever
 */
func handleTaskExecutionTimeout(taskid string, modelId string, workerId string) {
	log.Printf("[Scheduler][ ModelInference ][ handleTaskExecutionTimeout ]Setting timer to handle task execution timeout")
	conf := config.GetConfig("../../config/config.json")
	schedulerState.taskTimer[taskid] = time.NewTimer(time.Duration(conf.TaskExecutionTimeout) * time.Second)
	<-schedulerState.taskTimer[taskid].C

	modelname := schedulerState.GetModelName(modelId)

	log.Printf("[Scheduler][ ModelInference ][ handleTaskExecutionTimeout ]Task %v of model %v assigned to worker %v timed out; Updating its state to ready so that it can be scheduled again", taskid, modelname, workerId)

	schedulerState.HandleRescheduleOfTask(taskid)
}

/*
* Starts the scheduler process on the machine
 */
func StartSchedulerService(schedulerServicePort int, wg *sync.WaitGroup) {
	// Initialise the state of the scheduler
	schedulerState = &SchedulerState{
		lock:                &sync.RWMutex{},
		Models:              make(map[string]Model),
		Tasks:               make(map[string]Task),
		TaskQueue:           make(map[string]*ModelTasks),
		WindowOfTasks:       []string{},
		IndexIntoMemberList: 0,
		ModelNameToId:       make(map[string]string),
		taskTimer:           make(map[string]*time.Timer),
		queryRateDropTime:   time.Time{},
	}
	go queryRateMonitor()
	go SchedulerService_SyncWithSchedulerReplicas(wg)
	SchedulerService_IDunno(schedulerServicePort, wg)
}

/*
* RPC server handle for deploying model
 */
func (s *SchedulerServer) DeployModel(ctx context.Context, in *ss.DeployModelRequest) (*ss.DeployModelReply, error) {
	modelname := in.GetModelname()

	_, ok := schedulerState.GetModelId(modelname)
	if ok {
		return &ss.DeployModelReply{}, errors.New("Duplicated model name")
	}

	modelId, workers := schedulerState.AddModel(modelname)

	return &ss.DeployModelReply{
		ModelId: modelId,
		Workers: workers,
	}, nil
}

/*
* RPC server handle to handling acknowledgment of model deployment
 */
func (s *SchedulerServer) DeployModelAck(ctx context.Context, in *ss.DeployModelAckRequest) (*ss.DeployModelAckReply, error) {
	modelId := in.GetModelId()
	status := in.GetStatus()

	log.Printf("[ Scheduler ][ DeployModel ][DeployModelAck ]ModelId: %v; Status: %v", modelId, status)

	if status {
		schedulerState.MarkModelAsDeployed(modelId)
	} else {
		schedulerState.RemoveModel(modelId)
	}

	return &ss.DeployModelAckReply{}, nil
}

/*
* RPC server handler for scheduling new task of a model on a worker
 */
func (s *SchedulerServer) GimmeQuery(ctx context.Context, in *ss.GimmeQueryRequest) (*ss.GimmeQueryResponse, error) {
	modelId := in.GetModelId()
	workerId := in.GetWorkerId()

	log.Printf("[ Scheduler ][ Scheduling ][ GimmeQuery ]Got a request from worker: %v for a query on model: %v", workerId, schedulerState.GetModelName(modelId))

	modelStatus, ok := schedulerState.GetModelStatus(modelId)

	if !ok {
		log.Printf("[ Scheduler ][ Scheduling ][ GimmeQuery ]ModelId is invalid")
		return &ss.GimmeQueryResponse{
			Status:          false,
			Queryinputfiles: []string{},
		}, errors.New("ModelId is invalid")
	}

	if modelStatus == Undeployed {
		log.Printf("[ Scheduler ][ Scheduling ][ GimmeQuery ]Model isnt deployed yet")
		return &ss.GimmeQueryResponse{
			Status:          false,
			Queryinputfiles: []string{},
		}, errors.New("Model isnt deployed")
	}

	// check if there are any queries for the model that are in the ready state waiting to be scheduled and schedule them

	queryfilenames, taskId, ok := schedulerState.GetNextTaskToBeScheduled(modelId, workerId)

	return &ss.GimmeQueryResponse{
		Status:          ok,
		Queryinputfiles: queryfilenames,
		TaskId:          taskId,
	}, nil
}

/*
* RPC server handler for updating the query execution status as indicated by the worker
 */
func (s *SchedulerServer) UpdateQueryStatus(ctx context.Context, in *ss.UpdateQueryStatusRequest) (*ss.UpdateQueryStatusResponse, error) {
	taskId := in.GetTaskId()
	outputfiles := in.GetOutputfilenames()

	if len(outputfiles) == 0 {
		log.Printf("[ Scheduler ][ ModelInference ][ UpdateQueryStatus ]Model inference on the work FAILED")
	}

	log.Printf("[ Scheduler ][ ModelInference ][ UpdateQueryStatus ]Task %v completed with output file stored as %v", taskId, outputfiles)

	// Stopping the timer
	if _, ok := schedulerState.taskTimer[taskId]; ok {
		schedulerState.taskTimer[taskId].Stop()
	}

	if len(outputfiles) == 0 || !in.Status {
		schedulerState.HandleRescheduleOfTask(taskId)
	} else {
		schedulerState.MarkTaskComplete(taskId, outputfiles)
	}

	return &ss.UpdateQueryStatusResponse{}, nil
}

/*
* RPC server handler for clients submitting tasks
 */
func (s *SchedulerServer) SubmitTask(ctx context.Context, in *ss.SubmitTaskRequest) (*ss.SubmitTaskResponse, error) {
	modelname := in.GetModelname()
	queryInputFiles := in.GetQueryinputfiles()
	owner := in.GetOwner()
	creationTime := in.GetCreationtime()

	modelId, ok := schedulerState.GetModelId(modelname)

	if !ok {
		log.Printf("[ Scheduler ][ ModelInference ][ SubmitTask ]Model %v doesn't exist", modelname)
		return &ss.SubmitTaskResponse{},
			errors.New(fmt.Sprintf("Model %v doesn't exist", modelname))
	}

	log.Printf("[ Scheduler ][ ModelInference ][ SubmitTask ]Queueing query of input file %v on model %v::%v", queryInputFiles, modelname, modelId)
	taskId := schedulerState.QueueTask(modelname, modelId, queryInputFiles, owner, creationTime)

	return &ss.SubmitTaskResponse{
		TaskId: taskId,
	}, nil
}

/*
* RPC server handle to return all the tasks of a particular handle
 */
func (s *SchedulerServer) GetAllTasks(ctx context.Context, in *ss.GetAllTasksRequest) (*ss.GetAllTasksResponse, error) {
	owner := in.GetOwner()

	log.Printf("[ Scheduler ][ GetAllTasks ]Getting all the tasks of the owner: %v", owner)

	tasks := schedulerState.GetAllTasksOfClient(owner)
	log.Printf("[ Scheduler ][ GetAllTasks ]tasks: %v", tasks)

	if serialisedTasks, err := json.Marshal(tasks); err == nil {
		return &ss.GetAllTasksResponse{
			Tasks: serialisedTasks,
		}, nil
	}

	log.Fatalf("[ Scheduler ][ GetAllTasks ]Serialization of the tasks object failed")
	return &ss.GetAllTasksResponse{}, errors.New("Serialization of the tasks object failed")

}

/*
* RPC server handle to get all the tasks pertaining to a user for a model
 */
func (s *SchedulerServer) GetAllTasksOfModel(ctx context.Context, in *ss.GetAllTasksOfModelRequest) (*ss.GetAllTasksOfModelResponse, error) {
	owner := in.GetOwner()
	modelname := in.GetModelname()

	log.Printf("[ Scheduler ][ GetAllTasksOfModel ]Getting all the tasks of the owner: %v", owner)

	tasks := schedulerState.GetAllTasksOfModelOfClient(modelname, owner)

	if serialisedTasks, err := json.Marshal(tasks); err == nil {
		return &ss.GetAllTasksOfModelResponse{
			Tasks: serialisedTasks,
		}, nil
	}

	log.Fatalf("[ Scheduler ][ GetAllTasks ]Serialization of the tasks object failed")
	return &ss.GetAllTasksOfModelResponse{}, errors.New("Serialization of the tasks object failed")

}

/*
* RPC server handler to return all the query rates of all the models in the system
 */
func (s *SchedulerServer) GetAllQueryRates(ctx context.Context, in *ss.GetAllQueryRatesRequest) (*ss.GetAllQueryRatesResponse, error) {
	log.Printf("[ Scheduler ][ GetAllQueryRates ]")
	models, queryRates := schedulerState.getAllQueryRates()
	log.Printf("[ Scheduler ][ GetAllQueryRates ]models: %v; queryRates: %v", models, queryRates)
	return &ss.GetAllQueryRatesResponse{
		Modelnames: models,
		Queryrates: queryRates,
	}, nil
}

/*
* RPC handle to give information of all the models deployed in the system to the workers
* for model prefetching
 */
func (s *SchedulerServer) GimmeModels(ctx context.Context, in *ss.GimmeModelsRequest) (*ss.GimmeModelsResponse, error) {
	models := schedulerState.GetAllModels()

	return &ss.GimmeModelsResponse{
		Models: models,
	}, nil
}

/*
* Periodically updates the query rates of the models from a buffer of tasks
 */
func queryRateMonitor() {
	conf := config.GetConfig("../../config/config.json")
	for {
		if memberList == nil {
			continue
		}
		currentCoordinator := memberList.GetCoordinatorNode()
		myIpAddr := coordinatorState.myIpAddr

		if currentCoordinator == myIpAddr {
			schedulerState.UpdateModelsQueryRates()
		}
		time.Sleep(time.Duration(conf.QueryMonitorInterval) * time.Second)
	}
}

/*
* RPC Sever handle for serving the query counts of all the models
 */
func (s *SchedulerServer) GetQueryCount(ctx context.Context, in *ss.GetQueryCountRequest) (*ss.GetQueryCountResponse, error) {
	modelnames, querycounts := schedulerState.GetQueryCounts()

	log.Printf("[ Scheduler ][ GetQueryCount ]Query Count of models: %v are %v", modelnames, querycounts)

	return &ss.GetQueryCountResponse{
		Modelnames: modelnames,
		Querycount: querycounts,
	}, nil
}

/*
* RPC Sever handle for serving the query average execution time of all the models
 */
func (s *SchedulerServer) GetQueryAverageExectionTimes(ctx context.Context, in *ss.GetQueryAverageExectionTimeRequest) (*ss.GetQueryAverageExectionTimeResponse, error) {
	modelnames, exectimes := schedulerState.GetExectimes()

	log.Printf("[ Scheduler ][ GetQueryAverageExectionTimes ]Exec times of models: %v are %v", modelnames, exectimes)

	return &ss.GetQueryAverageExectionTimeResponse{
		Modelnames: modelnames,
		Exectimes:  exectimes,
	}, nil
}

/*
* Server handle for receiving and updating the scheduler state from the main scheduler
 */
func (s *SchedulerServer) SchedulerSync(ctx context.Context, in *ss.SchedulerSyncRequest) (*ss.SchedulerSyncResponse, error) {
	var models map[string]Model
	var tasks map[string]Task
	var modelNameId map[string]string

	unmarshallingError := json.Unmarshal(in.GetModels(), &models)
	if unmarshallingError != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error while unmarshalling the model: %v\n", unmarshallingError)

		return &ss.SchedulerSyncResponse{}, errors.New("Unmarshalling error(model)")
	}

	unmarshallingError = json.Unmarshal(in.GetTasks(), &tasks)
	if unmarshallingError != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error while unmarshalling the tasks: %v\n", unmarshallingError)

		return &ss.SchedulerSyncResponse{}, errors.New("Unmarshalling error(tasks)")
	}

	unmarshallingError = json.Unmarshal(in.GetModelNameToId(), &modelNameId)
	if unmarshallingError != nil {
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Error while unmarshalling the tasks: %v\n", unmarshallingError)

		return &ss.SchedulerSyncResponse{}, errors.New("Unmarshalling error(tasks)")
	}

	idx := in.GetIndexIntoMemberList()

	log.Printf("[ Scheduler ][ Scheduler Synchronisation ] Received State")
	go schedulerState.SetSchedulerState(models, tasks, modelNameId, idx)

	return &ss.SchedulerSyncResponse{}, nil
}

/*
* RPC handle for serving wokrers of a model
 */
func (s *SchedulerServer) GetWorkersOfModel(ctx context.Context, in *ss.GetWorkersOfModelRequest) (*ss.GetWorkersOfModelResponse, error) {
	modelname := in.GetModelname()

	log.Printf("[ Scheduler ][ GetWorkersOfModel ]Getting workers of the model: %v", modelname)

	modelId, ok := schedulerState.GetModelId(modelname)

	if !ok {
		log.Printf("[ Scheduler ][ GetWorkersOfModel ]Modelname is invalid")
		return &ss.GetWorkersOfModelResponse{}, errors.New("Model not found")
	}

	workers := schedulerState.GetWorkersOfModel(modelId)

	log.Printf("[ Scheduler ][ GetWorkersOfModel ]Workers of the model %v are %v", modelname, workers)

	return &ss.GetWorkersOfModelResponse{
		Workers: workers,
	}, nil
}

/*
* Synchronise the state of the main scheduler with the backup schedulers
 */
func SchedulerService_SyncWithSchedulerReplicas(wg *sync.WaitGroup) {
	conf := config.GetConfig("../../config/config.json")

	for {
		if memberList == nil {
			continue
		}
		currentCoordinator := memberList.GetCoordinatorNode()
		myIpAddr := coordinatorState.myIpAddr

		if currentCoordinator == myIpAddr {
			// fmt.Printf("[ Coordinator ][ Replica Recovery ]")
			log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Initialising\n")
			syncWithSchedulerReplicas()
		}
		time.Sleep(time.Duration(conf.SchedulerSyncTimer) * time.Second)
	}
}

/*
* Utility function to send snap of the states to all the backup schedulers
 */
func syncWithSchedulerReplicas() {
	allSchedulers := memberList.GetAllCoordinators()

	for _, scheduler := range allSchedulers {
		if scheduler == coordinatorState.myIpAddr {
			continue
		}
		sendStateSnapToBackupScheduler(scheduler)
	}
}

/*
* Utility function to send state of a scheduler to one backup scheduler
 */
func sendStateSnapToBackupScheduler(scheduler string) bool {
	conf := config.GetConfig("../../config/config.json")
	schedulerAddr := fmt.Sprintf("%v:%v", scheduler, conf.SchedulerPort)

	// log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Sending snap of my state to: %v", schedulerAddr)
	conn, err := grpc.Dial(schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Failed to establish connection with the scheduler: %v", err)
		return false
	}

	// defer conn.Close()

	// Initialise a client to connect to the coordinator process
	s := ss.NewSchedulerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	// defer cancel()

	defer conn.Close()
	defer cancel()

	models, tasks, modelNameId, index, ok := schedulerState.GetSnapOfSchedulerState()
	if !ok {
		return false
	}
	_, err = s.SchedulerSync(ctx, &ss.SchedulerSyncRequest{
		Models:              models,
		Tasks:               tasks,
		ModelNameToId:       modelNameId,
		IndexIntoMemberList: int32(index),
	})
	if err != nil {
		// may be service process is down
		log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Failed oopsss")
		return false
	}
	log.Printf("[ Scheduler ][ Scheduler Synchronisation ]Successfully sent the state to the backup scheduler %v", scheduler)
	return true
}
