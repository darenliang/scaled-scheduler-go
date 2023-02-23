package utils

type MessageTypeStatistics struct {
	Task               uint64 `json:"Task"`
	TaskEcho           uint64 `json:"TaskEcho"`
	TaskCancel         uint64 `json:"TaskCancel"`
	TaskCancelEcho     uint64 `json:"TaskCancelEcho"`
	TaskResult         uint64 `json:"TaskResult"`
	Heartbeat          uint64 `json:"Heartbeat"`
	FunctionRequest    uint64 `json:"FunctionRequest"`
	FunctionResponse   uint64 `json:"FunctionResponse"`
	MonitoringRequest  uint64 `json:"MonitoringRequest"`
	MonitoringResponse uint64 `json:"MonitoringResponse"`
}

type TaskManagerStatistics struct {
	Running    uint64 `json:"running"`
	Canceling  uint64 `json:"canceling"`
	Unassigned uint64 `json:"unassigned"`
	Failed     uint64 `json:"failed"`
	Canceled   uint64 `json:"canceled"`
}

type FunctionManagerStatistics struct {
	FunctionIDToTasks map[string]uint64 `json:"function_id_to_tasks"`
}

type WorkerManagerStatistics struct {
	Type          string            `json:"type"`
	WorkerToTasks map[string]uint64 `json:"worker_to_tasks"`
}

type SchedulerStatistics struct {
	Received        *MessageTypeStatistics     `json:"received"`
	Sent            *MessageTypeStatistics     `json:"sent"`
	TaskManager     *TaskManagerStatistics     `json:"task_manager"`
	FunctionManager *FunctionManagerStatistics `json:"function_manager"`
	WorkerManager   *WorkerManagerStatistics   `json:"worker_manager"`
}
