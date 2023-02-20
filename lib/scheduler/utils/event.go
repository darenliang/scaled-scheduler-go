// Package utils event.go is taken from https://github.com/trivigy/event
package utils

import (
	"context"
)

// Event is a communication primitive allowing for multiple goroutines to wait
// on an event to be set.
type Event struct {
	next chan chan struct{}
}

// NewEvent creates a new event instance.
func NewEvent(set bool) *Event {
	next := make(chan chan struct{}, 1)
	next <- make(chan struct{})
	event := &Event{next: next}
	if set {
		event.Set()
	}
	return event
}

// IsSet indicates whether an event is set or not.
func (r *Event) IsSet() bool {
	event := <-r.next
	r.next <- event
	return event == nil
}

// Set changes the internal state of an event to true.
func (r *Event) Set() {
	event := <-r.next
	if event != nil {
		close(event)
		event = nil
	}
	r.next <- event
}

// Clear resets the internal event state back to false.
func (r *Event) Clear() {
	event := <-r.next
	if event == nil {
		event = make(chan struct{})
	}
	r.next <- event
}

// Wait blocks until the event is set to true. If the event is already set,
// returns immediately. Otherwise, blocks until another goroutine sets the event.
func (r *Event) Wait(ctx context.Context) error {
	event := <-r.next
	r.next <- event
	if event != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-event:
		}
	}
	return nil
}
