package queue

type ChannelQueue[T any] struct {
	ch chan T
}

func NewChannelQueue[T any](cap int) *ChannelQueue[T] {
	return &ChannelQueue[T]{ch: make(chan T, cap)}
}

func (cq *ChannelQueue[T]) Len() int {
	return len(cq.ch)
}

func (cq *ChannelQueue[T]) Enqueue(v T) error {
	select {
	case cq.ch <- v:
		return nil
	default:
		return ErrQueueFull
	}
}

func (cq *ChannelQueue[T]) Dequeue() (T, error) {
	select {
	case v := <-cq.ch:
		return v, nil
	default:
		var zero T
		return zero, ErrQueueEmpty
	}
}
