package protocol

import "github.com/darenliang/scaled-scheduler-go/lib/interfaces"

func PackMessage(source string, msgType interfaces.Stringer, payload interfaces.Serializable) [][]byte {
	return append([][]byte{[]byte(source), []byte(msgType.String())}, payload.Serialize()...)
}
