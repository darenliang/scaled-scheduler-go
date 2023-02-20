package managers

type ClientManager struct {
	sendChan chan<- [][]byte
}

func NewClientManager(sendChan chan<- [][]byte) *ClientManager {
	return &ClientManager{
		sendChan: sendChan,
	}
}
