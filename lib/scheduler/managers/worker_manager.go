package managers

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type WorkerManager struct {
	sendChan             chan<- [][]byte
	perWorkerQueueSize   int
	workerTimeout        time.Duration
	taskManager          *TaskManager
	workerIDToAliveSince cmap.ConcurrentMap[string, time.Time]
	workerIDToTaskIDs    cmap.ConcurrentMap[string, cmap.ConcurrentMap[string, bool]]
	taskIDToWorkerID     cmap.ConcurrentMap[string, string]
	workerIDQueue        *utils.WorkerHeap
	workerIDQueueLock    sync.Mutex
}

func NewWorkerManager(sendChan chan<- [][]byte, perWorkerQueueSize int, workerTimeout time.Duration) *WorkerManager {
	return &WorkerManager{
		sendChan:             sendChan,
		perWorkerQueueSize:   perWorkerQueueSize,
		workerTimeout:        workerTimeout,
		workerIDToAliveSince: cmap.New[time.Time](),
		workerIDToTaskIDs:    cmap.New[cmap.ConcurrentMap[string, bool]](),
		taskIDToWorkerID:     cmap.New[string](),
		workerIDQueue:        utils.NewWorkerHeap(),
	}
}

func (m *WorkerManager) SetTaskManager(taskManager *TaskManager) {
	m.taskManager = taskManager
}

func (m *WorkerManager) OnHeartbeat(workerID string) {
	m.workerIDToAliveSince.Upsert(
		workerID,
		time.Now(),
		func(ok bool, _ time.Time, newValue time.Time) time.Time {
			if !ok {
				logging.Logger.Infof("worker %s connected", workerID)
				m.workerIDToTaskIDs.Set(workerID, cmap.New[bool]())
				m.workerIDQueueLock.Lock()
				heap.Push(m.workerIDQueue, &utils.WorkerHeapEntry{WorkerID: workerID, Tasks: 0})
				m.workerIDQueueLock.Unlock()
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
		m.workerIDQueueLock.Lock()
		if m.workerIDQueue.Len() == 0 {
			m.workerIDQueueLock.Unlock()
			return protocol.ErrNoWorkerAvailable
		}
		var entry *utils.WorkerHeapEntry

		// spinlock until we find a worker with available slots
		for {
			entry = m.workerIDQueue.Peek()
			if entry.Tasks < m.perWorkerQueueSize {
				break
			}

			// stop if context is cancelled
			if ctx.Err() != nil {
				m.workerIDQueueLock.Unlock()
				return ctx.Err()
			}

			m.workerIDQueueLock.Unlock()
			runtime.Gosched()
			m.workerIDQueueLock.Lock()
		}

		workerID = entry.WorkerID
		entry.Tasks++
		heap.Fix(m.workerIDQueue, 0)
		m.workerIDQueueLock.Unlock()
	}

	taskIDs, ok := m.workerIDToTaskIDs.Get(workerID)
	if !ok {
		return protocol.ErrWorkerNotFound
	}
	taskIDs.Set(task.TaskID, true)
	m.taskIDToWorkerID.Set(task.TaskID, workerID)
	m.sendChan <- protocol.PackMessage(workerID, protocol.MessageTypeTask, task)

	return nil
}

func (m *WorkerManager) OnTaskCancel(taskID string) error {
	workerID, ok := m.taskIDToWorkerID.Get(taskID)
	if !ok {
		return protocol.ErrTaskNotFound
	}
	m.sendChan <- protocol.PackMessage(workerID, protocol.MessageTypeTaskCancel, &protocol.TaskCancel{TaskID: taskID})
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

	m.workerIDQueueLock.Lock()
	m.workerIDQueue.AddTasks(workerID, -1)
	m.workerIDQueueLock.Unlock()

	m.taskManager.OnTaskDone(taskResult)

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

				m.workerIDQueueLock.Lock()
				entry, ok := m.workerIDQueue.WorkerIDToEntry[workerID]
				if ok {
					heap.Remove(m.workerIDQueue, entry.Index)
				}
				m.workerIDQueueLock.Unlock()

				taskIDs, ok := m.workerIDToTaskIDs.Pop(workerID)
				if ok {
					taskIDs.IterCb(func(taskID string, _ bool) {
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
