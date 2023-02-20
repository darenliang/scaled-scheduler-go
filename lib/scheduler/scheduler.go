package scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/managers"
	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/zeromq/goczmq"
)

type Scheduler struct {
	address               string
	perWorkerQueueSize    int
	workerTimeout         time.Duration
	functionRetentionTime time.Duration
	ctx                   context.Context
	wg                    *sync.WaitGroup
	cancel                context.CancelFunc
	router                *goczmq.Channeler
	clientManager         *managers.ClientManager
	functionManager       *managers.FunctionManager
	workerManager         *managers.WorkerManager
	taskManager           *managers.TaskManager
}

func NewScheduler(
	ctx context.Context,
	address string,
	perWorkerQueueSize int,
	workerTimeout, functionRetentionTime time.Duration,
) (*Scheduler, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	router := goczmq.NewRouterChanneler(
		address,
		goczmq.SockSetIdentity(fmt.Sprintf("S|%s|%d|%s", hostname, os.Getpid(), uuid.New().String())),
		goczmq.SockSetSndhwm(0),
		goczmq.SockSetRcvhwm(0),
	)

	// create context to cancel background goroutines
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	// initialize managers
	clientManager := managers.NewClientManager(router.SendChan)

	functionManager := managers.NewFunctionManager(router.SendChan, functionRetentionTime)
	go functionManager.RunGC(ctx, wg)
	wg.Add(1)

	workerManager := managers.NewWorkerManager(router.SendChan, perWorkerQueueSize, workerTimeout)
	go workerManager.RunGC(ctx, wg)
	wg.Add(1)

	taskManager := managers.NewTaskManager(router.SendChan)
	go taskManager.RunTaskAssignLoop(ctx, wg)
	wg.Add(1)

	// wire managers together
	workerManager.SetTaskManager(taskManager)
	taskManager.SetFunctionManager(functionManager)
	taskManager.SetWorkerManager(workerManager)

	return &Scheduler{
		address:               address,
		perWorkerQueueSize:    perWorkerQueueSize,
		workerTimeout:         workerTimeout,
		functionRetentionTime: functionRetentionTime,
		ctx:                   ctx,
		wg:                    wg,
		cancel:                cancel,
		router:                router,
		clientManager:         clientManager,
		functionManager:       functionManager,
		workerManager:         workerManager,
		taskManager:           taskManager,
	}, nil
}

func (s *Scheduler) Run() {
	logging.Logger.Info("scheduler started")

	pool, err := ants.NewPool(ants.DefaultAntsPoolSize)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	for {
		select {
		case msg := <-s.router.RecvChan:
			err := pool.Submit(func() { s.HandleMessage(msg) })
			if err != nil {
				logging.Logger.Error(err)
			}
		case err := <-s.router.ErrChan:
			logging.Logger.Error(err)
		case <-s.ctx.Done():
			s.cancel()
			pool.Release()
			s.wg.Wait()
			s.router.Destroy()
			logging.Logger.Info("scheduler exited")
			return
		}
	}
}

func (s *Scheduler) HandleMessage(msg [][]byte) {
	if len(msg) < 2 {
		logging.Logger.Errorf("received message only has %d frames", len(msg))
		return
	}

	source := string(msg[0])
	messageType := protocol.MessageType(msg[1])
	payload := msg[2:]

	switch messageType {
	case protocol.MessageTypeHeartbeat:
		s.HandleHeartbeat(source, payload)
	case protocol.MessageTypeMonitoringRequest:
		s.HandleMonitoringRequest(source, payload)
	case protocol.MessageTypeTask:
		s.HandleTask(source, payload)
	case protocol.MessageTypeTaskCancel:
		s.HandleTaskCancel(source, payload)
	case protocol.MessageTypeTaskResult:
		s.HandleTaskResult(source, payload)
	case protocol.MessageTypeFunctionRequest:
		s.HandleFunctionRequest(source, payload)
	default:
		logging.Logger.Errorf("received message has unsupported type %s", messageType)
	}
}

func (s *Scheduler) HandleHeartbeat(source string, payload [][]byte) {
	logging.Logger.Debugf("received heartbeat from %s", source)
	s.workerManager.OnHeartbeat(source)
}

func (s *Scheduler) HandleMonitoringRequest(source string, payload [][]byte) {
	logging.Logger.Debugf("received monitoring request from %s", source)
	logging.Logger.Warnf("monitoring request not implemented yet")
}

func (s *Scheduler) HandleFunctionRequest(source string, payload [][]byte) {
	logging.Logger.Debugf("received task cancel from %s", source)
	request, err := protocol.DeserializeFunctionRequest(payload)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
	s.functionManager.HandleFunctionRequest(source, request)
}

func (s *Scheduler) HandleTask(source string, payload [][]byte) {
	logging.Logger.Debugf("received task from %s", source)
	task, err := protocol.DeserializeTask(payload)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
	err = s.taskManager.OnTaskNew(source, task)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
}

func (s *Scheduler) HandleTaskCancel(source string, payload [][]byte) {
	logging.Logger.Debugf("received task cancel from %s", source)
	taskCancel, err := protocol.DeserializeTaskCancel(payload)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
	err = s.taskManager.OnTaskCancel(source, taskCancel.TaskID)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
}

func (s *Scheduler) HandleTaskResult(source string, payload [][]byte) {
	logging.Logger.Debugf("received task result from %s", source)
	taskResult, err := protocol.DeserializeTaskResult(payload)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
	err = s.workerManager.OnTaskDone(taskResult)
	if err != nil {
		logging.Logger.Error(err)
		return
	}
}
