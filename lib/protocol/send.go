package protocol

import (
	"github.com/darenliang/scaled-scheduler-go/lib/interfaces"
	"github.com/go-zeromq/zmq4"
)

func PackMessage(source string, msgType interfaces.Stringer, payload interfaces.Serializable) zmq4.Msg {
	return zmq4.Msg{Frames: append([][]byte{[]byte(source), []byte(msgType.String())}, payload.Serialize()...)}
}
