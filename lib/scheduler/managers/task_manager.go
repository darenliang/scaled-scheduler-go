package managers

import (
	"context"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type TaskQueueEntry struct {
	Task     *protocol.Task
	ClientID string
}

type TaskManager struct {
	router                    *protocol.Socket
	sentStatistics            *utils.MessageTypeStatistics
	functionManager           *FunctionManager
	workerManager             *WorkerManager
	taskIDToClientID          cmap.ConcurrentMap[uuid.UUID, string]
	taskIDToTask              cmap.ConcurrentMap[uuid.UUID, *protocol.Task]
	runningTaskIDs            cmap.ConcurrentMap[uuid.UUID, struct{}]
	cancelingTaskIDs          cmap.ConcurrentMap[uuid.UUID, struct{}]
	unassignedTaskQueue       chan *TaskQueueEntry
	unassignedTaskQueueLength uint64
	failedTaskCount           uint64
	canceledTaskCount         uint64
}

func NewTaskManager(router *protocol.Socket, sentStatistics *utils.MessageTypeStatistics) *TaskManager {
	return &TaskManager{
		router:              router,
		sentStatistics:      sentStatistics,
		taskIDToClientID:    cmap.NewStringer[uuid.UUID, string](),
		taskIDToTask:        cmap.NewStringer[uuid.UUID, *protocol.Task](),
		runningTaskIDs:      cmap.NewStringer[uuid.UUID, struct{}](),
		cancelingTaskIDs:    cmap.NewStringer[uuid.UUID, struct{}](),
		unassignedTaskQueue: make(chan *TaskQueueEntry),
	}
}

func (m *TaskManager) GetStatistics() *utils.TaskManagerStatistics {
	return &utils.TaskManagerStatistics{
		Running:    uint64(m.runningTaskIDs.Count()),
		Canceling:  uint64(m.cancelingTaskIDs.Count()),
		Unassigned: m.unassignedTaskQueueLength,
		Failed:     m.failedTaskCount,
		Canceled:   m.canceledTaskCount,
	}
}

func (m *TaskManager) SetFunctionManager(functionManager *FunctionManager) {
	m.functionManager = functionManager
}

func (m *TaskManager) SetWorkerManager(workerManager *WorkerManager) {
	m.workerManager = workerManager
}

func (m *TaskManager) OnTaskNew(clientID string, task *protocol.Task) error {
	defer atomic.AddUint64(&m.sentStatistics.TaskEcho, 1)

	if !m.functionManager.HasFunction(task.FunctionID) {
		logging.CheckError(m.router.Send(
			clientID, protocol.MessageTypeTaskEcho, &protocol.TaskEcho{
				TaskID: task.TaskID,
				Status: protocol.TaskEchoStatusFunctionNotExists,
			}))
		return nil
	}

	logging.CheckError(m.router.Send(
		clientID, protocol.MessageTypeTaskEcho, &protocol.TaskEcho{
			TaskID: task.TaskID,
			Status: protocol.TaskEchoStatusSubmitOK,
		}))

	err := m.functionManager.SetTaskUse(task.TaskID, task.FunctionID)
	if err != nil {
		return err
	}

	m.unassignedTaskQueue <- &TaskQueueEntry{ClientID: clientID, Task: task}
	atomic.AddUint64(&m.unassignedTaskQueueLength, 1)

	return nil
}

func (m *TaskManager) OnTaskReroute(taskID uuid.UUID) error {
	clientID, ok := m.taskIDToClientID.Pop(taskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	task, ok := m.taskIDToTask.Pop(taskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	m.runningTaskIDs.Remove(taskID)

	m.unassignedTaskQueue <- &TaskQueueEntry{ClientID: clientID, Task: task}
	atomic.AddUint64(&m.unassignedTaskQueueLength, 1)

	return nil
}

func (m *TaskManager) OnTaskCancel(clientID string, taskID uuid.UUID) error {
	defer atomic.AddUint64(&m.sentStatistics.TaskCancelEcho, 1)

	if m.cancelingTaskIDs.Has(taskID) {
		logging.Logger.Warnf("task %s is already being canceled", taskID)
		logging.CheckError(m.router.Send(
			clientID, protocol.MessageTypeTaskCancelEcho, &protocol.TaskCancelEcho{
				TaskID: taskID,
				Status: protocol.TaskEchoStatusDuplicated,
			}))

		return nil
	}

	if !m.runningTaskIDs.Has(taskID) {
		logging.Logger.Warnf("task %s is not running", taskID)
		logging.CheckError(m.router.Send(
			clientID, protocol.MessageTypeTaskCancelEcho, &protocol.TaskCancelEcho{
				TaskID: taskID,
				Status: protocol.TaskEchoStatusDuplicated,
			}))

		return nil
	}

	m.cancelingTaskIDs.Set(taskID, struct{}{})
	m.runningTaskIDs.Remove(taskID)

	logging.CheckError(m.router.Send(
		clientID, protocol.MessageTypeTaskCancelEcho, &protocol.TaskCancelEcho{
			TaskID: taskID,
			Status: protocol.TaskEchoStatusCancelOK,
		}))

	err := m.workerManager.OnTaskCancel(taskID)
	if err != nil {
		return err
	}

	return nil
}

func (m *TaskManager) OnTaskDone(result *protocol.TaskResult) error {
	switch result.Status {
	case protocol.TaskStatusSuccess:
		return m.OnTaskSuccess(result)
	case protocol.TaskStatusFailed:
		return m.OnTaskFailed(result)
	case protocol.TaskStatusCanceled:
		return m.OnTaskCanceled(result)
	default:
		return protocol.ErrUnknownTaskStatus
	}
}

func (m *TaskManager) OnAssignTask(ctx context.Context, entry *TaskQueueEntry) error {
	logging.Logger.Debugw("assigning task",
		"client_id", entry.ClientID,
		zap.Object("task", entry.Task),
	)

	m.taskIDToClientID.Set(entry.Task.TaskID, entry.ClientID)
	m.taskIDToTask.Set(entry.Task.TaskID, entry.Task)
	m.runningTaskIDs.Set(entry.Task.TaskID, struct{}{})

	err := m.workerManager.OnAssignTask(ctx, entry.Task)
	if err != nil {
		m.taskIDToClientID.Remove(entry.Task.TaskID)
		m.taskIDToTask.Remove(entry.Task.TaskID)
		m.runningTaskIDs.Remove(entry.Task.TaskID)
		return err
	}

	return nil
}

func (m *TaskManager) OnTaskSuccess(result *protocol.TaskResult) error {
	_, ok := m.runningTaskIDs.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	task, ok := m.taskIDToTask.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	clientID, _ := m.taskIDToClientID.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	logging.CheckError(m.router.Send(clientID, protocol.MessageTypeTaskResult, result))
	atomic.AddUint64(&m.sentStatistics.TaskResult, 1)

	err := m.functionManager.SetTaskDone(task.TaskID, task.FunctionID)
	if err != nil {
		return err
	}

	return nil
}

func (m *TaskManager) OnTaskFailed(result *protocol.TaskResult) error {
	_, ok := m.runningTaskIDs.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	task, ok := m.taskIDToTask.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	clientID, _ := m.taskIDToClientID.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	atomic.AddUint64(&m.failedTaskCount, 1)

	logging.CheckError(m.router.Send(clientID, protocol.MessageTypeTaskResult, result))
	atomic.AddUint64(&m.sentStatistics.TaskResult, 1)

	err := m.functionManager.SetTaskDone(task.TaskID, task.FunctionID)
	if err != nil {
		return err
	}

	return nil
}

func (m *TaskManager) OnTaskCanceled(result *protocol.TaskResult) error {
	_, ok := m.cancelingTaskIDs.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	task, ok := m.taskIDToTask.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	clientID, _ := m.taskIDToClientID.Pop(result.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	atomic.AddUint64(&m.canceledTaskCount, 1)

	logging.CheckError(m.router.Send(clientID, protocol.MessageTypeTaskResult, result))
	atomic.AddUint64(&m.sentStatistics.TaskResult, 1)

	err := m.functionManager.SetTaskDone(task.TaskID, task.FunctionID)
	if err != nil {
		return err
	}

	return nil
}

func (m *TaskManager) RunTaskAssignLoop(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case entry := <-m.unassignedTaskQueue:
			atomic.AddUint64(&m.unassignedTaskQueueLength, ^uint64(0)) // decrement by 1
			err := m.OnAssignTask(ctx, entry)
			if err != nil {
				logging.Logger.Errorf("assign task failed: %s", err.Error())
			}
		case <-ctx.Done():
			logging.Logger.Info("task manager assign loop stopped")
			wg.Done()
			return
		}
	}
}
