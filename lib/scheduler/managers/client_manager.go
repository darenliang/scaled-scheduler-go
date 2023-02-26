package managers

import (
	"github.com/darenliang/scaled-scheduler-go/lib/protocol"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
)

type ClientManager struct {
	router         *protocol.Socket
	sentStatistics *utils.MessageTypeStatistics
}

func NewClientManager(router *protocol.Socket, sentStatistics *utils.MessageTypeStatistics) *ClientManager {
	return &ClientManager{
		router:         router,
		sentStatistics: sentStatistics,
	}
}
