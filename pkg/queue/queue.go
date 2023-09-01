package queue

import (
	"errors"
)

var (
	ErrQueueEmpty = errors.New("queue empty")
	ErrQueueFull  = errors.New("queue full")
)

type Queue[T any] interface {
	Len() int
	Enqueue(v T) error
	Dequeue() (T, error)
	Clear()
}

func New[T any](cap int) Queue[T] {
	return NewChannelQueue[T](cap)
}
