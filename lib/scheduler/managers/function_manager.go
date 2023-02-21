package managers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type FunctionManager struct {
	sendChan               chan<- [][]byte
	functionRetention      time.Duration
	functionIDToAliveSince cmap.ConcurrentMap[string, time.Time]
	functionIDToFunction   cmap.ConcurrentMap[string, []byte]
	functionIDToTaskIDs    cmap.ConcurrentMap[string, cmap.ConcurrentMap[string, struct{}]]
}

func NewFunctionManager(sendChan chan<- [][]byte, functionRetention time.Duration) *FunctionManager {
	return &FunctionManager{
		sendChan:               sendChan,
		functionRetention:      functionRetention,
		functionIDToAliveSince: cmap.New[time.Time](),
		functionIDToFunction:   cmap.New[[]byte](),
		functionIDToTaskIDs:    cmap.New[cmap.ConcurrentMap[string, struct{}]](),
	}
}

func (m *FunctionManager) HasFunction(functionID string) bool {
	return m.functionIDToFunction.Has(functionID)
}

func (m *FunctionManager) AddFunction(functionID string, function []byte) error {
	m.functionIDToAliveSince.Set(functionID, time.Now())
	if m.HasFunction(functionID) {
		return protocol.ErrFunctionAlreadyExists
	}
	m.functionIDToFunction.Set(functionID, function)
	m.functionIDToTaskIDs.Set(functionID, cmap.New[struct{}]())
	return nil
}

func (m *FunctionManager) RemoveFunction(functionID string) error {
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

func (m *FunctionManager) SetTaskUse(taskID, functionID string) error {
	m.functionIDToAliveSince.Set(functionID, time.Now())
	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)
	if !ok {
		return protocol.ErrFunctionDoesNotExist
	}
	taskIDs.Set(taskID, struct{}{})
	return nil
}

func (m *FunctionManager) SetTaskDone(taskID, functionID string) error {
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

func (m *FunctionManager) OnFunctionCheck(source, functionID string) {
	logging.Logger.Debugf("received function check request from %s", source)
	if m.HasFunction(functionID) {
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeOK,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
		return
	}

	m.sendChan <- protocol.PackMessage(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeNotExists,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		})
}

func (m *FunctionManager) OnFunctionAdd(source, functionID string, function []byte) {
	logging.Logger.Debugf("received function add request from %s", source)
	err := m.AddFunction(functionID, function)
	if errors.Is(err, protocol.ErrFunctionAlreadyExists) {
		logging.Logger.Debugf("function %s already exists", functionID)
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeDuplicated,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
	}
	logging.Logger.Debugf("function %s added", functionID)
	m.sendChan <- protocol.PackMessage(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		})
}

func (m *FunctionManager) OnFunctionRequest(source, functionID string) {
	logging.Logger.Debugf("received function request from %s", source)
	function, ok := m.functionIDToFunction.Get(functionID)
	if !ok {
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
		return
	}
	m.sendChan <- protocol.PackMessage(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: function,
		})
}

func (m *FunctionManager) OnFunctionDelete(source, functionID string) {
	logging.Logger.Debugf("received function delete request from %s", source)
	if !m.HasFunction(functionID) {
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
		return
	}
	taskIDs, ok := m.functionIDToTaskIDs.Get(functionID)
	if !ok {
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeNotExists,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
		return
	}

	if taskIDs.Count() > 0 {
		m.sendChan <- protocol.PackMessage(
			source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
				Status:          protocol.FunctionResponseTypeStillHaveTask,
				FunctionID:      functionID,
				FunctionContent: make([]byte, 0),
			})
		return
	}

	m.RemoveFunction(functionID)
	m.sendChan <- protocol.PackMessage(
		source, protocol.MessageTypeFunctionResponse, &protocol.FunctionResponse{
			Status:          protocol.FunctionResponseTypeOK,
			FunctionID:      functionID,
			FunctionContent: make([]byte, 0),
		})
}

func (m *FunctionManager) RunGC(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-time.After(utils.GCSweepInterval):
			now := time.Now()
			deadFunctionIDs := make([]string, 0)
			m.functionIDToAliveSince.IterCb(func(functionID string, aliveSince time.Time) {
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
