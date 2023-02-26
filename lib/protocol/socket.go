package protocol

import (
	"github.com/darenliang/scaled-scheduler-go/lib/interfaces"
	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/go-zeromq/zmq4"
)

type Socket struct {
	ZmqSocket zmq4.Socket
}

func NewSocket(zmqSocket zmq4.Socket) *Socket {
	return &Socket{ZmqSocket: zmqSocket}
}

func (s *Socket) Send(source string, msgType interfaces.Stringer, payload interfaces.Serializable) error {
	logging.LogSendProtocolMessage(source, msgType, payload)
	return s.ZmqSocket.SendMulti(zmq4.Msg{Frames: append([][]byte{[]byte(source), []byte(msgType.String())}, payload.Serialize()...)})
}
