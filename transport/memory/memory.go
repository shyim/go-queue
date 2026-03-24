// Package memory provides an in-memory transport for testing.
package memory

import (
	"context"
	"sync"

	"github.com/shyim/go-queue"
)

// Transport is an in-memory [queue.Transport] for testing.
type Transport struct {
	mu      sync.Mutex
	ch      chan *queue.Envelope
	acked   []*queue.Envelope
	nacked  []*queue.Envelope
	setupOK bool
}

// NewTransport creates a new in-memory transport.
func NewTransport() *Transport {
	return &Transport{
		ch: make(chan *queue.Envelope, 100),
	}
}

func (t *Transport) Setup(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setupOK = true
	return nil
}

func (t *Transport) Send(_ context.Context, envelope *queue.Envelope) error {
	t.ch <- envelope
	return nil
}

func (t *Transport) Receive(ctx context.Context) (<-chan *queue.Envelope, error) {
	out := make(chan *queue.Envelope)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case env, ok := <-t.ch:
				if !ok {
					return
				}
				select {
				case out <- env:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (t *Transport) Ack(_ context.Context, envelope *queue.Envelope) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.acked = append(t.acked, envelope)
	return nil
}

func (t *Transport) Nack(_ context.Context, envelope *queue.Envelope, requeue bool) error {
	t.mu.Lock()
	t.nacked = append(t.nacked, envelope)
	t.mu.Unlock()
	if requeue {
		t.ch <- envelope
	}
	return nil
}

func (t *Transport) Retry(_ context.Context, envelope *queue.Envelope) error {
	t.mu.Lock()
	t.acked = append(t.acked, envelope)
	t.mu.Unlock()
	t.ch <- envelope
	return nil
}

// Acked returns a copy of all acknowledged envelopes.
func (t *Transport) Acked() []*queue.Envelope {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*queue.Envelope, len(t.acked))
	copy(out, t.acked)
	return out
}

// Nacked returns a copy of all negatively acknowledged envelopes.
func (t *Transport) Nacked() []*queue.Envelope {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*queue.Envelope, len(t.nacked))
	copy(out, t.nacked)
	return out
}

// SetupDone reports whether Setup has been called.
func (t *Transport) SetupDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.setupOK
}

// Chan returns the raw internal channel for test inspection.
func (t *Transport) Chan() <-chan *queue.Envelope {
	return t.ch
}
