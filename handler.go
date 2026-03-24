package queue

import "context"

// Handler processes messages of type T.
type Handler[T any] interface {
	Handle(ctx context.Context, msg T) error
}

// HandlerFunc is an adapter to use ordinary functions as handlers.
type HandlerFunc[T any] func(ctx context.Context, msg T) error

func (f HandlerFunc[T]) Handle(ctx context.Context, msg T) error {
	return f(ctx, msg)
}
