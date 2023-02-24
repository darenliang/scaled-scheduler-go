package managers

import (
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler/utils"
	"github.com/go-zeromq/zmq4"
)

type ClientManager struct {
	router         zmq4.Socket
	sentStatistics *utils.MessageTypeStatistics
}

func NewClientManager(router zmq4.Socket, sentStatistics *utils.MessageTypeStatistics) *ClientManager {
	return &ClientManager{
		router:         router,
		sentStatistics: sentStatistics,
	}
}
