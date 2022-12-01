package process

import (
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type SchedulerServer struct {
	ss.UnimplementedSchedulerServiceServer
}

var (
	schedulerState *SchedulerState
)

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

type Model struct {
	Name         string
	Id           string
	Workers      []string
	QueryRate    float64
	CreationTime time.Time
	Status       ModelStatus
}

func (model Model) String() string {
	return fmt.Sprintf("(ModelName, %v), (ModelId, %v), (Workers, %v), (CreationTime, %v), (Status, %v)", model.Name, model.Id, model.Workers, model.CreationTime, model.Status)
}

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

type SchedulerState struct {
	sync.RWMutex
	Models              map[string]Model
	Tasks               map[string]Task     // all the tasks in the system
	TaskQueue           map[string][]string //model-id to task-id mapping
	WindowOfTasks       []string
	IndexIntoMemberList int
	ModelNameToId       map[string]string
	TaskTimer           map[string]*time.Timer
}

func (state *SchedulerState) GetModelName(modelId string) string {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelName ]Invalid Model Id")
		return ""
	}

	return state.Models[modelId].Name
}

func (state *SchedulerState) GetModelId(modelname string) (string, bool) {
	state.RLock()
	defer state.RUnlock()

	modelId, ok := state.ModelNameToId[modelname]

	return modelId, ok
}

func (state *SchedulerState) GetAllTasksOfClient(clientId string) []Task {
	state.RLock()
	defer state.RUnlock()

	tasks := []Task{}

	for _, task := range state.Tasks {
		if task.OwnerId == clientId {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

func (state *SchedulerState) GetAllTasksOfModelOfClient(modelname string, clientId string) []Task {
	state.RLock()
	defer state.RUnlock()

	tasks := []Task{}

	for _, task := range state.Tasks {
		if task.OwnerId == clientId && task.ModelName == modelname {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

func (state *SchedulerState) GetQueryRateOfModel(modelId string) float64 {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetQueryRateOfModel ]Invalid Model Id")
		return -1
	}

	return state.Models[modelId].QueryRate
}

func (state *SchedulerState) AddModel(modelname string) (string, []string) {
	state.Lock()
	defer state.Unlock()
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

	return modelId, nodes
}

func (state *SchedulerState) QueueTask(modelname string, modelId string, queryinputfiles []string, owner string, creationTime string) string {
	state.Lock()
	defer state.Unlock()

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
	}

	log.Printf("[ Scheduler ][ ModelInference ]Adding task: %v to queue of model %v::%v", task, modelname, modelId)

	state.Tasks[taskId] = task
	if _, ok := state.TaskQueue[modelId]; !ok {
		state.TaskQueue[modelId] = []string{}
	}
	state.TaskQueue[modelId] = append(state.TaskQueue[modelId], taskId)

	return taskId
}

func (state *SchedulerState) MarkModelAsDeployed(modelId string) {
	state.Lock()
	defer state.Unlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Invalid Model Id")
		return
	}
	log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Model %v deployed", modelId)
	model := state.Models[modelId]
	model.Status = Deployed

	state.Models[modelId] = model
}

func (state *SchedulerState) RemoveModel(modelId string) {
	state.Lock()
	defer state.Unlock()

	log.Printf("[]")

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ RemoveModel ]Invalid Model Id")

		return
	}
	log.Printf("[ Scheduler ][ RemoveModel ]Removed the model %v from the set of active models", modelId)
	delete(state.Models, modelId)
}

func (state *SchedulerState) GetModelStatus(modelId string) (ModelStatus, bool) {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.Models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelStatus ]Invalid Model Id")
		return -1, false
	}

	return state.Models[modelId].Status, true
}

func (state *SchedulerState) GetNextTaskToBeScheduled(modelId string, workerId string) ([]string, string, bool) {
	state.Lock()
	defer state.Unlock()

	log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]Model: %v; Worker: %v", modelId, workerId)
	tasksofModel := state.TaskQueue[modelId]
	log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]TaskQueue[%v]: %v", modelId, tasksofModel)

	for _, taskid := range tasksofModel {
		if state.Tasks[taskid].Status == Ready {
			task := state.Tasks[taskid]
			log.Printf("[ Scheduler ][ ModelInference ][ GetNextTaskToBeScheduled ]Next task to be allocated for model %v::%v is %v", modelId, task.ModelName, taskid)
			task.AssignedTo = workerId
			task.StartTime = time.Now()
			task.Status = Waiting

			// Start execution timeout timer for this task
			go handleTaskExecutionTimeout(taskid, modelId, workerId)

			// Appending the task to the window of tasks
			state.WindowOfTasks = append(state.WindowOfTasks, taskid)

			state.Tasks[taskid] = task

			return task.Filenames, taskid, true
		}
	}

	return []string{}, "", false
}

func (state *SchedulerState) HandleRescheduleOfTask(taskId string) {
	state.Lock()
	defer state.Unlock()

	task := state.Tasks[taskId]
	task.AssignedTo = ""
	task.StartTime = time.Time{}
	task.Status = Ready

	state.Tasks[taskId] = task

	log.Printf("[ Scheduler ][ ModelInference ][ HandleRescheduleOfTask ]Updated state of the task %v to Ready", taskId)
}

func (state *SchedulerState) MarkTaskComplete(taskId string, outputFiles []string) {
	state.Lock()
	defer state.Unlock()

	task := state.Tasks[taskId]

	task.Status = Success
	task.EndTime = time.Now()
	task.Resultfilenames = outputFiles

	state.Tasks[taskId] = task

	log.Printf("[ Scheduler ][ ModelInference ][ MarkTaskComplete ]Marked task %v of model %v as complete; Output file: %v", taskId, task.ModelName, outputFiles)
}

func handleTaskExecutionTimeout(taskid string, modelId string, workerId string) {
	log.Printf("[Scheduler][ ModelInference ][ handleTaskExecutionTimeout ]Setting timer to handle task execution timeout")
	conf := config.GetConfig("../../config/config.json")
	schedulerState.TaskTimer[taskid] = time.NewTimer(time.Duration(conf.TaskExecutionTimeout) * time.Second)
	<-schedulerState.TaskTimer[taskid].C

	modelname := schedulerState.GetModelName(modelId)

	log.Printf("[Scheduler][ ModelInference ][ handleTaskExecutionTimeout ]Task %v of model %v assigned to worker %v timed out; Updating its state to ready so that it can be scheduled again", taskid, modelname, workerId)

	schedulerState.HandleRescheduleOfTask(taskid)
}

func StartSchedulerService(schedulerServicePort int, wg *sync.WaitGroup) {
	// Initialise the state of the scheduler
	schedulerState = &SchedulerState{
		Models:              make(map[string]Model),
		Tasks:               make(map[string]Task),
		TaskQueue:           make(map[string][]string),
		WindowOfTasks:       []string{},
		IndexIntoMemberList: 0,
		ModelNameToId:       make(map[string]string),
		TaskTimer:           make(map[string]*time.Timer),
	}
	SchedulerService_IDunno(schedulerServicePort, wg)
}

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

func (s *SchedulerServer) UpdateQueryStatus(ctx context.Context, in *ss.UpdateQueryStatusRequest) (*ss.UpdateQueryStatusResponse, error) {
	taskId := in.GetTaskId()
	outputfiles := in.GetOutputfilenames()

	if len(outputfiles) == 0 {
		log.Printf("[ Scheduler ][ ModelInference ][ UpdateQueryStatus ]Model inference on the work FAILED")
	}

	log.Printf("[ Scheduler ][ ModelInference ][ UpdateQueryStatus ]Task %v completed with output file stored as %v", taskId, outputfiles)

	// Stopping the timer
	schedulerState.TaskTimer[taskId].Stop()

	if len(outputfiles) == 0 {
		schedulerState.HandleRescheduleOfTask(taskId)
	} else {
		schedulerState.MarkTaskComplete(taskId, outputfiles)
	}

	return &ss.UpdateQueryStatusResponse{}, nil
}

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
