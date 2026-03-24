package queue

import "context"

// Transport is the interface that message brokers must implement.
type Transport interface {
	Sender
	Receiver
}

// Sender can send envelopes to a message broker.
type Sender interface {
	Send(ctx context.Context, envelope *Envelope) error
}

// Receiver can consume envelopes from a message broker.
type Receiver interface {
	Receive(ctx context.Context) (<-chan *Envelope, error)
	Ack(ctx context.Context, envelope *Envelope) error
	Nack(ctx context.Context, envelope *Envelope, requeue bool) error
	// Retry acks the original delivery and re-publishes the envelope
	// with updated retry metadata so the attempt count survives the broker round-trip.
	Retry(ctx context.Context, envelope *Envelope) error
}

// SetupTransport can auto-create its infrastructure (exchanges, queues, etc.).
type SetupTransport interface {
	Setup(ctx context.Context) error
}
