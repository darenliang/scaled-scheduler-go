package utils

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pebbe/zmq4"
)

type RouterChanneler struct {
	SendCh  chan [][]byte
	RecvCh  chan [][]byte
	ErrCh   chan error
	closeCh chan struct{}
	closed  bool
}

func NewRouterChanneler(endpoint string, identity string) (*RouterChanneler, error) {
	// unique identifier used by inproc sockets
	id := uuid.New().String()

	// create router socket to receive and send messages
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return nil, err
	}
	err = routerSocket.SetIdentity(identity)
	if err != nil {
		return nil, err
	}
	err = routerSocket.SetSndhwm(0)
	if err != nil {
		return nil, err
	}
	err = routerSocket.SetRcvhwm(0)
	if err != nil {
		return nil, err
	}
	err = routerSocket.Bind(endpoint)
	if err != nil {
		return nil, err
	}

	// create pull socket to process message sending requests
	pullMsgSocket, err := zmq4.NewSocket(zmq4.PULL)
	if err != nil {
		return nil, err
	}
	err = pullMsgSocket.Bind(fmt.Sprintf("inproc://proxy_%s", id))
	if err != nil {
		return nil, err
	}

	// create push socket to send message sending requests
	pushMsgSocket, err := zmq4.NewSocket(zmq4.PUSH)
	if err != nil {
		return nil, err
	}
	err = pushMsgSocket.Connect(fmt.Sprintf("inproc://proxy_%s", id))
	if err != nil {
		return nil, err
	}

	// create pull socket to process close requests
	pullCloseSocket, err := zmq4.NewSocket(zmq4.PULL)
	if err != nil {
		return nil, err
	}
	err = pullCloseSocket.Bind(fmt.Sprintf("inproc://close_%s", id))
	if err != nil {
		return nil, err
	}

	// create push socket to send close requests
	pushCloseSocket, err := zmq4.NewSocket(zmq4.PUSH)
	if err != nil {
		return nil, err
	}
	err = pushCloseSocket.Connect(fmt.Sprintf("inproc://close_%s", id))
	if err != nil {
		return nil, err
	}

	rc := &RouterChanneler{
		SendCh:  make(chan [][]byte),
		RecvCh:  make(chan [][]byte),
		ErrCh:   make(chan error),
		closeCh: make(chan struct{}, 1),
	}

	go rc.socketHandler(routerSocket, pullMsgSocket, pullCloseSocket)
	go rc.messagePusher(pushMsgSocket, pushCloseSocket)

	return rc, nil
}

func (rc *RouterChanneler) Close() {
	if rc.closed {
		return
	}
	rc.closeCh <- struct{}{}
}

func (rc *RouterChanneler) socketHandler(routerSocket, pullMsgSocket, pullCloseSocket *zmq4.Socket) {
	defer routerSocket.Close()
	defer pullMsgSocket.Close()
	defer pullCloseSocket.Close()

	poller := zmq4.NewPoller()
	poller.Add(routerSocket, zmq4.POLLIN)
	poller.Add(pullMsgSocket, zmq4.POLLIN)
	poller.Add(pullCloseSocket, zmq4.POLLIN)
	for {
		sockets, err := poller.Poll(-1)
		if err != nil {
			rc.ErrCh <- err
			continue
		}

		for _, socket := range sockets {
			switch socket.Socket {
			case routerSocket:
				msg, err := routerSocket.RecvMessageBytes(0)
				if err != nil {
					rc.ErrCh <- err
					continue
				}
				rc.RecvCh <- msg

			case pullMsgSocket:
				msg, err := pullMsgSocket.RecvMessageBytes(0)
				if err != nil {
					rc.ErrCh <- err
					continue
				}

				_, err = routerSocket.SendMessage(msg)
				if err != nil {
					rc.ErrCh <- err
					continue
				}

			case pullCloseSocket:
				return
			}
		}
	}
}

func (rc *RouterChanneler) messagePusher(pushMsgSocket, pushCloseSocket *zmq4.Socket) {
	defer pushMsgSocket.Close()
	defer pushCloseSocket.Close()

	for {
		select {
		case msg := <-rc.SendCh:
			_, err := pushMsgSocket.SendMessage(msg)
			if err != nil {
				rc.ErrCh <- err
			}
		case <-rc.closeCh:
			rc.closed = true
			_, err := pushCloseSocket.SendMessage("")
			if err != nil {
				rc.ErrCh <- err
			}
			return
		}
	}
}
