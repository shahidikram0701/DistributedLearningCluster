package process

import (
	"context"
	"cs425/mp/config"
	ss "cs425/mp/proto/scheduler_proto"
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
	name         string
	id           string
	workers      []string
	queryRate    float64
	creationTime time.Time
	status       ModelStatus
}

func (model Model) String() string {
	return fmt.Sprintf("(ModelName, %v), (ModelId, %v), (Workers, %v), (CreationTime, %v), (Status, %v)", model.name, model.id, model.workers, model.creationTime, model.status)
}

type Task struct {
	id           string
	name         string
	status       TaskStatus
	creationTime time.Time
	assignedTo   string
	modelId      string
	startTime    time.Time
	endTime      time.Time
	ownerId      string
	filename     string
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

type SchedulerState struct {
	sync.RWMutex
	models              map[string]Model
	tasks               []Task
	taskQueue           map[string][]string //model-id to task-id mapping
	windowOfTasks       []string
	indexIntoMemberList int
}

func (state *SchedulerState) GetModelName(modelId string) string {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelName ]Invalid Model Id")
		return ""
	}

	return state.models[modelId].name
}

func (state *SchedulerState) GetAllTasksOfClient(clientId string) []Task {
	state.RLock()
	defer state.RUnlock()

	tasks := []Task{}

	for _, task := range state.tasks {
		if task.ownerId == clientId {
			tasks = append(tasks, task)
		}
	}

	return tasks
}

func (state *SchedulerState) GetQueryRateOfModel(modelId string) float64 {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetQueryRateOfModel ]Invalid Model Id")
		return -1
	}

	return state.models[modelId].queryRate
}

func (state *SchedulerState) AddModel(modelname string) (string, []string) {
	state.Lock()
	defer state.Unlock()
	conf := config.GetConfig("../../config/config.json")

	modelId := uuid.New().String()
	log.Printf("[ Scheduler ][ DeployModel ][ AddModel ]New modelId for model %v: %v", modelname, modelId)

	nodes, newIndexIntoMemberlist := memberList.GetNDataNodes(state.indexIntoMemberList, conf.NumOfWorkersPerModel)
	state.indexIntoMemberList = newIndexIntoMemberlist

	log.Printf("[ Scheduler ][ DeployModel ][ AddModel ]Allocated workers for model %v with modelId %v: %v", modelname, modelId, nodes)

	model := Model{
		name:         modelname,
		id:           modelId,
		queryRate:    0.0,
		creationTime: time.Now(),
		workers:      nodes,
		status:       Undeployed,
	}

	state.models[modelId] = model

	return modelId, nodes
}

func (state *SchedulerState) MarkModelAsDeployed(modelId string) {
	state.Lock()
	defer state.Unlock()

	if _, ok := state.models[modelId]; !ok {
		log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Invalid Model Id")
		return
	}
	log.Printf("[ Scheduler ][ MarkModelAsDeployed ]Model %v deployed", modelId)
	model := state.models[modelId]
	model.status = Deployed

	state.models[modelId] = model
}

func (state *SchedulerState) RemoveModel(modelId string) {
	state.Lock()
	defer state.Unlock()

	log.Printf("[]")

	if _, ok := state.models[modelId]; !ok {
		log.Printf("[ Scheduler ][ RemoveModel ]Invalid Model Id")

		return
	}
	log.Printf("[ Scheduler ][ RemoveModel ]Removed the model %v from the set of active models", modelId)
	delete(state.models, modelId)
}

func (state *SchedulerState) GetModelStatus(modelId string) (ModelStatus, bool) {
	state.RLock()
	defer state.RUnlock()

	if _, ok := state.models[modelId]; !ok {
		log.Printf("[ Scheduler ][ GetModelStatus ]Invalid Model Id")
		return -1, false
	}

	return state.models[modelId].status, true
}

func (s *SchedulerServer) DeployModel(ctx context.Context, in *ss.DeployModelRequest) (*ss.DeployModelReply, error) {
	modelname := in.GetModelname()

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
			Status:         false,
			Queryinputfile: "",
		}, errors.New("ModelId is invalid")
	}

	if modelStatus == Undeployed {
		log.Printf("[ Scheduler ][ Scheduling ][ GimmeQuery ]Model isnt deployed yet")
		return &ss.GimmeQueryResponse{
			Status:         false,
			Queryinputfile: "",
		}, errors.New("Model isnt deployed")
	}

	// check if there are any queries for the model that are in the ready state waiting to be scheduled and schedule them

	return &ss.GimmeQueryResponse{
		Status:         false,
		Queryinputfile: "",
	}, nil
}
