package managers

import (
	"container/heap"
	"context"
	"github.com/marusama/semaphore/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type WorkerManager struct {
	sendChan             chan<- [][]byte
	sentStatistics       *utils.MessageTypeStatistics
	perWorkerQueueSize   int
	workerTimeout        time.Duration
	taskManager          *TaskManager
	workerIDToAliveSince cmap.ConcurrentMap[string, time.Time]
	workerIDToTaskIDs    cmap.ConcurrentMap[string, cmap.ConcurrentMap[string, struct{}]]
	taskIDToWorkerID     cmap.ConcurrentMap[string, string]
	workerQueue          *utils.WorkerHeap
	workerQueueLock      sync.Mutex
	workerQueueSemaphore semaphore.Semaphore
}

func NewWorkerManager(
	sendChan chan<- [][]byte,
	sentStatistics *utils.MessageTypeStatistics,
	perWorkerQueueSize int,
	workerTimeout time.Duration,
) *WorkerManager {
	return &WorkerManager{
		sendChan:             sendChan,
		sentStatistics:       sentStatistics,
		perWorkerQueueSize:   perWorkerQueueSize,
		workerTimeout:        workerTimeout,
		workerIDToAliveSince: cmap.New[time.Time](),
		workerIDToTaskIDs:    cmap.New[cmap.ConcurrentMap[string, struct{}]](),
		taskIDToWorkerID:     cmap.New[string](),
		workerQueue:          utils.NewWorkerHeap(),
		workerQueueSemaphore: semaphore.New(0),
	}
}

func (m *WorkerManager) GetStatistics() *utils.WorkerManagerStatistics {
	workerToTasks := make(map[string]uint64)
	m.workerIDToTaskIDs.IterCb(func(k string, v cmap.ConcurrentMap[string, struct{}]) {
		workerToTasks[k] = uint64(v.Count())
	})
	return &utils.WorkerManagerStatistics{
		Type:          "priority_queue",
		WorkerToTasks: workerToTasks,
	}
}

func (m *WorkerManager) SetTaskManager(taskManager *TaskManager) {
	m.taskManager = taskManager
}

func (m *WorkerManager) OnHeartbeat(workerID string) {
	m.workerIDToAliveSince.Upsert(
		workerID,
		time.Now(),
		func(exist bool, _, newValue time.Time) time.Time {
			if !exist {
				logging.Logger.Infof("worker %s connected", workerID)
				m.workerIDToTaskIDs.Set(workerID, cmap.New[struct{}]())
				m.workerQueueLock.Lock()
				heap.Push(m.workerQueue, &utils.WorkerHeapEntry{WorkerID: workerID, Tasks: 0})
				m.workerQueueSemaphore.SetLimit(m.workerQueue.Len() * m.perWorkerQueueSize)
				m.workerQueueLock.Unlock()
			}
			return newValue
		},
	)
}

func (m *WorkerManager) OnAssignTask(ctx context.Context, task *protocol.Task) error {
	var workerID string
	if id, ok := m.taskIDToWorkerID.Get(task.TaskID); ok {
		workerID = id
	} else {
		m.workerQueueLock.Lock()
		if m.workerQueue.Len() == 0 {
			m.workerQueueLock.Unlock()
			return protocol.ErrNoWorkerAvailable
		}

		m.workerQueueLock.Unlock()
		err := m.workerQueueSemaphore.Acquire(ctx, 1)
		if err != nil {
			return ctx.Err()
		}
		m.workerQueueLock.Lock()

		entry := m.workerQueue.Peek()
		workerID = entry.WorkerID
		entry.Tasks++
		heap.Fix(m.workerQueue, 0)
		m.workerQueueLock.Unlock()
	}

	taskIDs, ok := m.workerIDToTaskIDs.Get(workerID)
	if !ok {
		return protocol.ErrWorkerNotFound
	}
	taskIDs.Set(task.TaskID, struct{}{})

	m.taskIDToWorkerID.Set(task.TaskID, workerID)
	m.sendChan <- protocol.PackMessage(workerID, protocol.MessageTypeTask, task)
	atomic.AddUint64(&m.sentStatistics.Task, 1)

	return nil
}

func (m *WorkerManager) OnTaskCancel(taskID string) error {
	workerID, ok := m.taskIDToWorkerID.Get(taskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	m.sendChan <- protocol.PackMessage(workerID, protocol.MessageTypeTaskCancel, &protocol.TaskCancel{TaskID: taskID})
	atomic.AddUint64(&m.sentStatistics.TaskCancel, 1)

	return nil
}

func (m *WorkerManager) OnTaskDone(taskResult *protocol.TaskResult) error {
	workerID, ok := m.taskIDToWorkerID.Pop(taskResult.TaskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}

	taskIDs, ok := m.workerIDToTaskIDs.Get(workerID)
	if !ok {
		return protocol.ErrWorkerNotFound
	}
	taskIDs.Remove(taskResult.TaskID)

	m.workerQueueLock.Lock()
	m.workerQueue.AddTasks(workerID, -1)
	m.workerQueueSemaphore.Release(1)
	m.workerQueueLock.Unlock()

	err := m.taskManager.OnTaskDone(taskResult)
	if err != nil {
		return err
	}

	return nil
}

func (m *WorkerManager) RunGC(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-time.After(utils.GCSweepInterval):
			now := time.Now()
			deadWorkerIDs := make([]string, 0)
			m.workerIDToAliveSince.IterCb(func(workerID string, aliveSince time.Time) {
				if now.Sub(aliveSince) > m.workerTimeout {
					deadWorkerIDs = append(deadWorkerIDs, workerID)
				}
			})

			deadWorkersGCed := 0
			for _, workerID := range deadWorkerIDs {
				m.workerIDToAliveSince.Remove(workerID)

				m.workerQueueLock.Lock()
				entry, ok := m.workerQueue.WorkerIDToEntry[workerID]
				if ok {
					heap.Remove(m.workerQueue, entry.Index)
				}
				m.workerQueueSemaphore.SetLimit(m.workerQueue.Len() * m.perWorkerQueueSize)
				m.workerQueueLock.Unlock()

				taskIDs, ok := m.workerIDToTaskIDs.Pop(workerID)
				if ok {
					taskIDs.IterCb(func(taskID string, _ struct{}) {
						m.taskIDToWorkerID.Remove(taskID)
						m.taskManager.OnTaskReroute(taskID)
					})
				} else {
					logging.Logger.Warnf("worker %s not found in worker id to task id mapping", workerID)
				}
				deadWorkersGCed++
			}

			if deadWorkersGCed > 0 {
				logging.Logger.Infof("worker manager GC removed %d inactive workers", deadWorkersGCed)
			}
		case <-ctx.Done():
			logging.Logger.Info("worker manager GC stopped")
			wg.Done()
			return
		}
	}
}
