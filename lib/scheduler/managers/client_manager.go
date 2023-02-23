package managers

import "github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"

type ClientManager struct {
	sendChan       chan<- [][]byte
	sentStatistics *utils.MessageTypeStatistics
}

func NewClientManager(sendChan chan<- [][]byte, sentStatistics *utils.MessageTypeStatistics) *ClientManager {
	return &ClientManager{
		sendChan:       sendChan,
		sentStatistics: sentStatistics,
	}
}
