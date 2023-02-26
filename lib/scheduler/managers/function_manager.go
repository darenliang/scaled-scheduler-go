package managers

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type FunctionManager struct {
	router                 *protocol.Socket
	sentStatistics         *utils.MessageTypeStatistics
	functionRetention      time.Duration
	functionIDToAliveSince cmap.ConcurrentMap[uuid.UUID, time.Time]
	functionIDToFunction   cmap.ConcurrentMap[uuid.UUID, []byte]
	functionIDToTaskIDs    cmap.ConcurrentMap[uuid.UUID, cmap.ConcurrentMap[uuid.UUID, struct{}]]
}

func NewFunctionManager(
	router *protocol.Socket,
	sentStatistics *utils.MessageTypeStatistics,
	functionRetention time.Duration,
) *FunctionManager {
	return &FunctionManager{
		router:                 router,
		sentStatistics:         sentStatistics,
		functionRetention:      functionRetention,
		functionIDToAliveSince: cmap.NewStringer[uuid.UUID, time.Time](),
		functionIDToFunction:   cmap.NewStringer[uuid.UUID, []byte](),
		functionIDToTaskIDs:    cmap.NewStringer[uuid.UUID, cmap.ConcurrentMap[uuid.UUID, struct{}]](),
	}
}

func (m *FunctionManager) GetStatistics() *utils.FunctionManagerStatistics {
	functionIDToTasks := make(map[uuid.UUID]uint64)
	m.functionIDToTaskIDs.IterCb(func(k uuid.UUID, v cmap.ConcurrentMap[uuid.UUID, struct{}]) {
		functionIDToTasks[k] = uint64(v.Count())
	})

	return &utils.FunctionManagerStatistics{FunctionIDToTasks: functionIDToTasks}
}

func (m *FunctionManager) HasFunction(functionID uuid.UUID) bool {
	return m.functionIDToFunction.Has(functionID)
}

func (m *FunctionManager) AddFunction(functionID uuid.UUID, function []byte) error {
	m.functionIDToAliveSince.Set(functionID, time.Now())
	if m.HasFunction(functionID) {
		return protocol.ErrFunctionAlreadyExists
	}

	m.functionIDToFunction.Set(functionID, function)
	m.functionIDToTaskIDs.Set(functionID, cmap.NewStringer[uuid.UUID, struct{}]())

	return nil
}

func (m *FunctionManager) RemoveFunction(functionID uuid.UUID) error {
	if !m.HasFunction(functionID) {
		return protocol.ErrFunctionDoesNotExist
	}

	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)
	if !ok {
		return protocol.ErrFunctionDoesNotExist
	}

	if taskIDs.Count() != 0 {
		return protocol.ErrFunctionStillHaveTasks
	}

	m.functionIDToAliveSince.Remove(functionID)
	m.functionIDToFunction.Remove(functionID)
	m.functionIDToTaskIDs.Remove(functionID)

	return nil
}

func (m *FunctionManager) SetTaskUse(taskID, functionID uuid.UUID) error {
	m.functionIDToAliveSince.Set(functionID, time.Now())

	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)
	if !ok {
		return protocol.ErrFunctionDoesNotExist
	}

	taskIDs.Set(taskID, struct{}{})

	return nil
}

func (m *FunctionManager) SetTaskDone(taskID, functionID uuid.UUID) error {
	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)

	if !ok {
		return protocol.ErrFunctionDoesNotExist
	}

	taskIDs.Remove(taskID)

	return nil
}

func (m *FunctionManager) HandleFunctionRequest(source string, request *protocol.FunctionRequest) {
	switch request.Type {
	case protocol.FunctionRequestTypeCheck:
		m.OnFunctionCheck(source, request.FunctionID)
	case protocol.FunctionRequestTypeAdd:
		m.OnFunctionAdd(source, request.FunctionID, request.FunctionContent)
	case protocol.FunctionRequestTypeRequest:
		m.OnFunctionRequest(source, request.FunctionID)
	case protocol.FunctionRequestTypeDelete:
		m.OnFunctionDelete(source, request.FunctionID)
	}
}

func (m *FunctionManager) OnFunctionCheck(source string, functionID uuid.UUID) {
	defer atomic.AddUint64(&m.sentStatistics.FunctionResponse, 1)

	if m.HasFunction(functionID) {
		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeOK,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))
		return
	}

	logging.CheckError(m.router.Send(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeNotExists,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		}))
}

func (m *FunctionManager) OnFunctionAdd(source string, functionID uuid.UUID, function []byte) {
	defer atomic.AddUint64(&m.sentStatistics.FunctionResponse, 1)

	err := m.AddFunction(functionID, function)
	if errors.Is(err, protocol.ErrFunctionAlreadyExists) {

		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeDuplicated,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))

		return
	}

	logging.CheckError(m.router.Send(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		}))
}

func (m *FunctionManager) OnFunctionRequest(source string, functionID uuid.UUID) {
	defer atomic.AddUint64(&m.sentStatistics.FunctionResponse, 1)

	function, ok := m.functionIDToFunction.Get(functionID)
	if !ok {
		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))
		return
	}

	logging.CheckError(m.router.Send(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: function,
		}))
}

func (m *FunctionManager) OnFunctionDelete(source string, functionID uuid.UUID) {
	defer atomic.AddUint64(&m.sentStatistics.FunctionResponse, 1)

	if !m.HasFunction(functionID) {
		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))
		return
	}

	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)
	if !ok {
		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))
		return
	}

	if taskIDs.Count() > 0 {
		logging.CheckError(m.router.Send(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeStillHaveTask,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			}))
		return
	}

	m.RemoveFunction(functionID)

	logging.CheckError(m.router.Send(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		}))
}

func (m *FunctionManager) RunGC(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-time.After(utils.GCSweepInterval):
			now := time.Now()
			deadFunctionIDs := make([]uuid.UUID, 0)
			m.functionIDToAliveSince.IterCb(func(functionID uuid.UUID, aliveSince time.Time) {
				if now.Sub(aliveSince) > m.functionRetention {
					deadFunctionIDs = append(deadFunctionIDs, functionID)
				}
			})

			deadFunctionsGCed := 0
			for _, functionID := range deadFunctionIDs {
				err := m.RemoveFunction(functionID)
				if err == nil {
					deadFunctionsGCed++
				}
			}

			if deadFunctionsGCed > 0 {
				logging.Logger.Infof("function manager GC removed %d unused functions", deadFunctionsGCed)
			}
		case <-ctx.Done():
			logging.Logger.Info("function manager GC stopped")
			wg.Done()
			return
		}
	}
}
