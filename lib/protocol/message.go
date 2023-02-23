package protocol

import (
	"encoding/binary"
	"math"
)

type MessageType string

func (mt MessageType) String() string {
	return string(mt)
}

const (
	MessageTypeTask               MessageType = "TK"
	MessageTypeTaskEcho           MessageType = "TE"
	MessageTypeTaskCancel         MessageType = "TC"
	MessageTypeTaskCancelEcho     MessageType = "TX"
	MessageTypeTaskResult         MessageType = "TR"
	MessageTypeHeartbeat          MessageType = "HB"
	MessageTypeFunctionRequest    MessageType = "FR"
	MessageTypeFunctionResponse   MessageType = "FA"
	MessageTypeMonitoringRequest  MessageType = "MR"
	MessageTypeMonitoringResponse MessageType = "MS"
)

var MessageTypeSet = map[MessageType]struct{}{
	MessageTypeTask:               {},
	MessageTypeTaskEcho:           {},
	MessageTypeTaskCancel:         {},
	MessageTypeTaskCancelEcho:     {},
	MessageTypeTaskResult:         {},
	MessageTypeHeartbeat:          {},
	MessageTypeFunctionRequest:    {},
	MessageTypeFunctionResponse:   {},
	MessageTypeMonitoringRequest:  {},
	MessageTypeMonitoringResponse: {},
}

type TaskStatus string

func (ts TaskStatus) String() string {
	return string(ts)
}

const (
	TaskStatusSuccess  TaskStatus = "S"
	TaskStatusFailed   TaskStatus = "F"
	TaskStatusCanceled TaskStatus = "C"
)

var TaskStatusSet = map[TaskStatus]struct{}{
	TaskStatusSuccess:  {},
	TaskStatusFailed:   {},
	TaskStatusCanceled: {},
}

type TaskEchoStatus string

func (tes TaskEchoStatus) String() string {
	return string(tes)
}

const (
	TaskEchoStatusSubmitOK          TaskEchoStatus = "SK"
	TaskEchoStatusCancelOK          TaskEchoStatus = "CK"
	TaskEchoStatusDuplicated        TaskEchoStatus = "DC"
	TaskEchoStatusFunctionNotExists TaskEchoStatus = "FN"
)

var TaskEchoStatusSet = map[TaskEchoStatus]struct{}{
	TaskEchoStatusSubmitOK:          {},
	TaskEchoStatusCancelOK:          {},
	TaskEchoStatusDuplicated:        {},
	TaskEchoStatusFunctionNotExists: {},
}

type FunctionRequestType string

func (frt FunctionRequestType) String() string {
	return string(frt)
}

const (
	FunctionRequestTypeCheck   FunctionRequestType = "C"
	FunctionRequestTypeAdd     FunctionRequestType = "A"
	FunctionRequestTypeRequest FunctionRequestType = "R"
	FunctionRequestTypeDelete  FunctionRequestType = "D"
)

var FunctionRequestTypeSet = map[FunctionRequestType]struct{}{
	FunctionRequestTypeCheck:   {},
	FunctionRequestTypeAdd:     {},
	FunctionRequestTypeRequest: {},
	FunctionRequestTypeDelete:  {},
}

type FunctionResponseType string

func (frt FunctionResponseType) String() string {
	return string(frt)
}

const (
	FunctionResponseTypeOK            FunctionResponseType = "OK"
	FunctionResponseTypeNotExists     FunctionResponseType = "NE"
	FunctionResponseTypeStillHaveTask FunctionResponseType = "HT"
	FunctionResponseTypeDuplicated    FunctionResponseType = "DC"
)

var FunctionResponseTypeSet = map[FunctionResponseType]struct{}{
	FunctionResponseTypeOK:            {},
	FunctionResponseTypeNotExists:     {},
	FunctionResponseTypeStillHaveTask: {},
	FunctionResponseTypeDuplicated:    {},
}

type Task struct {
	TaskID          string
	FunctionID      string
	FunctionContent []byte
	FunctionArgs    []byte
}

func (t *Task) Serialize() [][]byte {
	return [][]byte{[]byte(t.TaskID), []byte(t.FunctionID), t.FunctionContent, t.FunctionArgs}
}

func DeserializeTask(data [][]byte) (*Task, error) {
	if len(data) != 4 {
		return nil, ErrInvalidDataLength
	}

	return &Task{
		TaskID:          string(data[0]),
		FunctionID:      string(data[1]),
		FunctionContent: data[2],
		FunctionArgs:    data[3],
	}, nil
}

type TaskEcho struct {
	TaskID string
	Status TaskEchoStatus
}

func (te *TaskEcho) Serialize() [][]byte {
	return [][]byte{[]byte(te.TaskID), []byte(te.Status)}
}

func DeserializeTaskEcho(data [][]byte) (*TaskEcho, error) {
	if len(data) != 2 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskEchoStatusSet[TaskEchoStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	return &TaskEcho{
		TaskID: string(data[0]),
		Status: TaskEchoStatus(data[1]),
	}, nil
}

type TaskCancel struct {
	TaskID string
}

func (tc *TaskCancel) Serialize() [][]byte {
	return [][]byte{[]byte(tc.TaskID)}
}

func DeserializeTaskCancel(data [][]byte) (*TaskCancel, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	return &TaskCancel{
		TaskID: string(data[0]),
	}, nil
}

type TaskCancelEcho struct {
	TaskID string
	Status TaskEchoStatus
}

func (tce *TaskCancelEcho) Serialize() [][]byte {
	return [][]byte{[]byte(tce.TaskID), []byte(tce.Status)}
}

func DeserializeTaskCancelEcho(data [][]byte) (*TaskCancelEcho, error) {
	if len(data) != 2 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskEchoStatusSet[TaskEchoStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	return &TaskCancelEcho{
		TaskID: string(data[0]),
		Status: TaskEchoStatus(data[1]),
	}, nil
}

type TaskResult struct {
	TaskID   string
	Status   TaskStatus
	Result   []byte
	Duration float32
}

func (tr *TaskResult) Serialize() [][]byte {
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, math.Float32bits(tr.Duration))
	return [][]byte{[]byte(tr.TaskID), []byte(tr.Status), buffer, tr.Result}
}

func DeserializeTaskResult(data [][]byte) (*TaskResult, error) {
	if len(data) != 4 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskStatusSet[TaskStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	return &TaskResult{
		TaskID:   string(data[0]),
		Status:   TaskStatus(data[1]),
		Duration: math.Float32frombits(binary.LittleEndian.Uint32(data[2])),
		Result:   data[3],
	}, nil
}

type Heartbeat struct {
	cpuUsage float32
	rssSize  uint64
}

func (h *Heartbeat) Serialize() [][]byte {
	buffer := make([]byte, 12)
	binary.LittleEndian.PutUint32(buffer[:4], math.Float32bits(h.cpuUsage))
	binary.LittleEndian.PutUint64(buffer[4:], h.rssSize)
	return [][]byte{buffer}
}

func DeserializeHeartbeat(data [][]byte) (*Heartbeat, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	return &Heartbeat{
		cpuUsage: math.Float32frombits(binary.LittleEndian.Uint32(data[0][:4])),
		rssSize:  binary.LittleEndian.Uint64(data[0][4:]),
	}, nil
}

type MonitorRequest struct{}

func (mr *MonitorRequest) Serialize() [][]byte {
	return [][]byte{{}}
}

func DeserializeMonitorRequest(data [][]byte) (*MonitorRequest, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	return &MonitorRequest{}, nil
}

type MonitorResponse struct {
	Data []byte
}

func (mr *MonitorResponse) Serialize() [][]byte {
	return [][]byte{mr.Data}
}

func DeserializeMonitorResponse(data [][]byte) (*MonitorResponse, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	return &MonitorResponse{Data: data[0]}, nil
}

type FunctionRequest struct {
	Type            FunctionRequestType
	FunctionID      string
	FunctionContent []byte
}

func (fr *FunctionRequest) Serialize() [][]byte {
	return [][]byte{[]byte(fr.Type), []byte(fr.FunctionID), fr.FunctionContent}
}

func DeserializeFunctionRequest(data [][]byte) (*FunctionRequest, error) {
	if len(data) != 3 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := FunctionRequestTypeSet[FunctionRequestType(data[0])]; !ok {
		return nil, ErrInvalidEnum
	}

	return &FunctionRequest{
		Type:            FunctionRequestType(data[0]),
		FunctionID:      string(data[1]),
		FunctionContent: data[2],
	}, nil
}

type FunctionResponse struct {
	Status          FunctionResponseType
	FunctionID      string
	FunctionContent []byte
}

func (fr *FunctionResponse) Serialize() [][]byte {
	return [][]byte{[]byte(fr.Status), []byte(fr.FunctionID), fr.FunctionContent}
}

func DeserializeFunctionResponse(data [][]byte) (*FunctionResponse, error) {
	if len(data) != 3 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := FunctionResponseTypeSet[FunctionResponseType(data[0])]; !ok {
		return nil, ErrInvalidEnum
	}

	return &FunctionResponse{
		Status:          FunctionResponseType(data[0]),
		FunctionID:      string(data[1]),
		FunctionContent: data[2],
	}, nil
}
