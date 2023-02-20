package protocol

import "errors"

var (
	ErrInvalidDataLength = errors.New("invalid data length")
	ErrInvalidEnum       = errors.New("invalid enum")
)

var (
	ErrFunctionAlreadyExists  = errors.New("function already exists")
	ErrFunctionDoesNotExist   = errors.New("function does not exist")
	ErrFunctionStillHaveTasks = errors.New("function still have tasks")
	ErrTaskNotFound           = errors.New("task not found")
	ErrWorkerNotFound         = errors.New("worker not found")
	ErrNoWorkerAvailable      = errors.New("no worker available")
	ErrUnknownTaskStatus      = errors.New("unknown task status")
)
