package utils

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pebbe/zmq4"
)

type RouterChanneler struct {
	identity string
	id       string
	endpoint string
	closeCh  chan struct{}
	closed   bool
	SendCh   chan [][]byte
	RecvCh   chan [][]byte
	ErrCh    chan error
}

func NewRouterChanneler(endpoint string, identity string) *RouterChanneler {
	rc := &RouterChanneler{
		identity: identity,
		id:       uuid.New().String(),
		endpoint: endpoint,
		closeCh:  make(chan struct{}, 1),
		SendCh:   make(chan [][]byte),
		RecvCh:   make(chan [][]byte),
		ErrCh:    make(chan error),
	}

	go rc.socketHandler()
	go rc.messagePusher()

	return rc
}

func (rc *RouterChanneler) Close() {
	if rc.closed {
		return
	}
	rc.closeCh <- struct{}{}
}

func (rc *RouterChanneler) socketHandler() {
	defer close(rc.RecvCh)

	// create router socket to receive messages
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = routerSocket.SetIdentity(rc.identity)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = routerSocket.SetSndhwm(0)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = routerSocket.SetRcvhwm(0)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = routerSocket.Bind(rc.endpoint)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	defer routerSocket.Close()

	// create pull socket to proxy messages from message pusher
	proxySocket, err := zmq4.NewSocket(zmq4.PULL)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = proxySocket.Bind(fmt.Sprintf("inproc://proxy_%s", rc.id))
	if err != nil {
		rc.ErrCh <- err
		return
	}
	defer proxySocket.Close()

	// create pair socket to receive a close message
	closeSocket, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = closeSocket.Bind(fmt.Sprintf("inproc://close_%s", rc.id))
	if err != nil {
		rc.ErrCh <- err
		return
	}
	defer closeSocket.Close()

	poller := zmq4.NewPoller()
	poller.Add(routerSocket, zmq4.POLLIN)
	poller.Add(proxySocket, zmq4.POLLIN)
	poller.Add(closeSocket, zmq4.POLLIN)
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

			case proxySocket:
				msg, err := proxySocket.RecvMessageBytes(0)
				if err != nil {
					rc.ErrCh <- err
					continue
				}

				_, err = routerSocket.SendMessage(msg)
				if err != nil {
					rc.ErrCh <- err
					continue
				}

			case closeSocket:
				return
			}
		}
	}
}

func (rc *RouterChanneler) messagePusher() {
	// create push socket to send messages to socket handler
	proxySocket, err := zmq4.NewSocket(zmq4.PUSH)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = proxySocket.Connect(fmt.Sprintf("inproc://proxy_%s", rc.id))
	if err != nil {
		rc.ErrCh <- err
		return
	}
	defer proxySocket.Close()

	// create pair socket to emit close messages
	closeSocket, err := zmq4.NewSocket(zmq4.PAIR)
	if err != nil {
		rc.ErrCh <- err
		return
	}
	err = closeSocket.Connect(fmt.Sprintf("inproc://close_%s", rc.id))
	if err != nil {
		rc.ErrCh <- err
		return
	}
	defer closeSocket.Close()

	for {
		select {
		case msg := <-rc.SendCh:
			_, err := proxySocket.SendMessage(msg)
			if err != nil {
				rc.ErrCh <- err
			}
		case <-rc.closeCh:
			rc.closed = true
			_, err := closeSocket.SendMessage("")
			if err != nil {
				rc.ErrCh <- err
			}
			return
		}
	}
}
