package protocol

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/google/uuid"
	"go.uber.org/zap/zapcore"
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
	TaskID          uuid.UUID // TODO: task is a binary uuid
	FunctionID      uuid.UUID
	FunctionContent []byte
	FunctionArgs    []byte
}

func (t *Task) Serialize() [][]byte {
	return [][]byte{t.TaskID[:], []byte(hex.EncodeToString(t.FunctionID[:])), t.FunctionContent, t.FunctionArgs}
}

func (t *Task) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("task_id", t.TaskID.String())
	enc.AddString("function_id", t.FunctionID.String())
	enc.AddInt("function_content_len", len(t.FunctionContent))
	enc.AddInt("function_args_len", len(t.FunctionArgs))
	return nil
}

func DeserializeTask(data [][]byte) (*Task, error) {
	if len(data) != 4 {
		return nil, ErrInvalidDataLength
	}

	taskID, err := uuid.FromBytes(data[0])
	if err != nil {
		return nil, err
	}

	functionID, err := uuid.ParseBytes(data[1])
	if err != nil {
		return nil, err
	}

	return &Task{
		TaskID:          taskID,
		FunctionID:      functionID,
		FunctionContent: data[2],
		FunctionArgs:    data[3],
	}, nil
}

type TaskEcho struct {
	TaskID uuid.UUID // TODO: task is a binary uuid
	Status TaskEchoStatus
}

func (te *TaskEcho) Serialize() [][]byte {
	return [][]byte{te.TaskID[:], []byte(te.Status)}
}

func (te *TaskEcho) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("task_id", te.TaskID.String())
	enc.AddString("status", te.Status.String())
	return nil
}

func DeserializeTaskEcho(data [][]byte) (*TaskEcho, error) {
	if len(data) != 2 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskEchoStatusSet[TaskEchoStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	taskID, err := uuid.FromBytes(data[0])
	if err != nil {
		return nil, err
	}

	return &TaskEcho{
		TaskID: taskID,
		Status: TaskEchoStatus(data[1]),
	}, nil
}

type TaskCancel struct {
	TaskID uuid.UUID // TODO: task is a binary uuid
}

func (tc *TaskCancel) Serialize() [][]byte {
	return [][]byte{tc.TaskID[:]}
}

func (tc *TaskCancel) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("task_id", tc.TaskID.String())
	return nil
}

func DeserializeTaskCancel(data [][]byte) (*TaskCancel, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	taskID, err := uuid.FromBytes(data[0])
	if err != nil {
		return nil, err
	}

	return &TaskCancel{
		TaskID: taskID,
	}, nil
}

type TaskCancelEcho struct {
	TaskID uuid.UUID // TODO: task is a binary uuid
	Status TaskEchoStatus
}

func (tce *TaskCancelEcho) Serialize() [][]byte {
	return [][]byte{tce.TaskID[:], []byte(tce.Status)}
}

func (tce *TaskCancelEcho) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("task_id", tce.TaskID.String())
	enc.AddString("status", tce.Status.String())
	return nil
}

func DeserializeTaskCancelEcho(data [][]byte) (*TaskCancelEcho, error) {
	if len(data) != 2 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskEchoStatusSet[TaskEchoStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	taskID, err := uuid.FromBytes(data[0])
	if err != nil {
		return nil, err
	}

	return &TaskCancelEcho{
		TaskID: taskID,
		Status: TaskEchoStatus(data[1]),
	}, nil
}

type TaskResult struct {
	TaskID   uuid.UUID // TODO: task is a binary uuid
	Status   TaskStatus
	Result   []byte
	Duration float32
}

func (tr *TaskResult) Serialize() [][]byte {
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, math.Float32bits(tr.Duration))
	return [][]byte{tr.TaskID[:], []byte(tr.Status), buffer, tr.Result}
}

func (tr *TaskResult) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("task_id", tr.TaskID.String())
	enc.AddString("status", tr.Status.String())
	enc.AddInt("result_len", len(tr.Result))
	enc.AddFloat32("duration", tr.Duration)
	return nil
}

func DeserializeTaskResult(data [][]byte) (*TaskResult, error) {
	if len(data) != 4 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := TaskStatusSet[TaskStatus(data[1])]; !ok {
		return nil, ErrInvalidEnum
	}

	taskID, err := uuid.FromBytes(data[0])
	if err != nil {
		return nil, err
	}

	return &TaskResult{
		TaskID:   taskID,
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

func (h *Heartbeat) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddFloat32("cpu_usage", h.cpuUsage)
	enc.AddUint64("rss_size", h.rssSize)
	return nil
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

func (mr *MonitorRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
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

func (mr *MonitorResponse) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("data_len", len(mr.Data))
	return nil
}

func DeserializeMonitorResponse(data [][]byte) (*MonitorResponse, error) {
	if len(data) != 1 {
		return nil, ErrInvalidDataLength
	}

	return &MonitorResponse{Data: data[0]}, nil
}

type FunctionRequest struct {
	Type            FunctionRequestType
	FunctionID      uuid.UUID
	FunctionContent []byte
}

func (fr *FunctionRequest) Serialize() [][]byte {
	return [][]byte{[]byte(fr.Type), []byte(hex.EncodeToString(fr.FunctionID[:])), fr.FunctionContent}
}

func (fr *FunctionRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", fr.Type.String())
	enc.AddString("function_id", fr.FunctionID.String())
	enc.AddInt("function_content_len", len(fr.FunctionContent))
	return nil
}

func DeserializeFunctionRequest(data [][]byte) (*FunctionRequest, error) {
	if len(data) != 3 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := FunctionRequestTypeSet[FunctionRequestType(data[0])]; !ok {
		return nil, ErrInvalidEnum
	}

	functionID, err := uuid.ParseBytes(data[1])
	if err != nil {
		return nil, err
	}

	return &FunctionRequest{
		Type:            FunctionRequestType(data[0]),
		FunctionID:      functionID,
		FunctionContent: data[2],
	}, nil
}

type FunctionResponse struct {
	Status          FunctionResponseType
	FunctionID      uuid.UUID
	FunctionContent []byte
}

func (fr *FunctionResponse) Serialize() [][]byte {
	return [][]byte{[]byte(fr.Status), []byte(hex.EncodeToString(fr.FunctionID[:])), fr.FunctionContent}
}

func (fr *FunctionResponse) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("status", fr.Status.String())
	enc.AddString("function_id", fr.FunctionID.String())
	enc.AddInt("function_content_len", len(fr.FunctionContent))
	return nil
}

func DeserializeFunctionResponse(data [][]byte) (*FunctionResponse, error) {
	if len(data) != 3 {
		return nil, ErrInvalidDataLength
	}

	if _, ok := FunctionResponseTypeSet[FunctionResponseType(data[0])]; !ok {
		return nil, ErrInvalidEnum
	}

	functionID, err := uuid.ParseBytes(data[1])
	if err != nil {
		return nil, err
	}

	return &FunctionResponse{
		Status:          FunctionResponseType(data[0]),
		FunctionID:      functionID,
		FunctionContent: data[2],
	}, nil
}
