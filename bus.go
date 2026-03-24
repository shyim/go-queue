package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

type handlerRegistration struct {
	handler   any
	msgType   reflect.Type
	transport string
}

// Bus dispatches messages to transports and routes them to handlers.
type Bus struct {
	mu                  sync.RWMutex
	transports          map[string]Transport
	handlers            map[string]*handlerRegistration
	dispatchMiddleware  []Middleware
}

// NewBus creates a new message bus.
func NewBus() *Bus {
	return &Bus{
		transports: make(map[string]Transport),
		handlers:   make(map[string]*handlerRegistration),
	}
}

// AddTransport registers a named transport with the bus.
func (b *Bus) AddTransport(name string, t Transport) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.transports[name] = t
}

// AddDispatchMiddleware registers middleware that wraps every Dispatch call.
// Middleware receives the fully built envelope before it is sent to the transport.
func (b *Bus) AddDispatchMiddleware(mw ...Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dispatchMiddleware = append(b.dispatchMiddleware, mw...)
}

func messageTypeName[T any]() string {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + "." + t.Name()
}

// Handle registers a handler for message type T on the named transport.
func Handle[T any](b *Bus, transportName string, handler Handler[T]) {
	typeName := messageTypeName[T]()
	var zero T
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[typeName] = &handlerRegistration{
		handler:   handler,
		msgType:   reflect.TypeOf(zero),
		transport: transportName,
	}
}

// HandleFunc registers a function as a handler for message type T.
func HandleFunc[T any](b *Bus, transportName string, fn func(ctx context.Context, msg T) error) {
	Handle[T](b, transportName, HandlerFunc[T](fn))
}

// Dispatch sends a message of type T to its configured transport.
func Dispatch[T any](ctx context.Context, b *Bus, msg T, opts ...Option) error {
	o := newDispatchOptions(opts...)
	typeName := messageTypeName[T]()

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue: failed to marshal message: %w", err)
	}

	envelope := &Envelope{
		Body:    body,
		Type:    typeName,
		ID:      uuid.New().String(),
		Headers: make(map[string]string),
	}

	if o.delay > 0 {
		envelope.AddStamp(DelayStamp{Delay: o.delay})
	}
	if o.maxRetries != nil {
		envelope.AddStamp(RedeliveryStamp{MaxRetries: *o.maxRetries})
	}
	for k, v := range o.headers {
		envelope.Headers[k] = v
	}

	b.mu.RLock()
	reg, ok := b.handlers[typeName]
	b.mu.RUnlock()

	transportName := ""
	if ok {
		transportName = reg.transport
	}
	if o.queue != "" {
		transportName = o.queue
	}
	if transportName == "" {
		return fmt.Errorf("queue: no transport configured for message type %s", typeName)
	}

	b.mu.RLock()
	transport, ok := b.transports[transportName]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("queue: transport %q not found", transportName)
	}

	envelope.Transport = transportName
	envelope.AddStamp(SentStamp{
		TransportName: transportName,
		SentAt:        time.Now(),
	})

	send := func(ctx context.Context, env *Envelope) error {
		return transport.Send(ctx, env)
	}

	b.mu.RLock()
	mws := b.dispatchMiddleware
	b.mu.RUnlock()

	chain := send
	for i := len(mws) - 1; i >= 0; i-- {
		mw := mws[i]
		next := chain
		chain = func(ctx context.Context, env *Envelope) error {
			return mw(ctx, env, next)
		}
	}

	return chain(ctx, envelope)
}

func (b *Bus) GetHandler(typeName string) (*handlerRegistration, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	reg, ok := b.handlers[typeName]
	return reg, ok
}

func (b *Bus) getTransport(name string) (Transport, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.transports[name]
	return t, ok
}

// Setup calls Setup on all transports that implement SetupTransport.
func (b *Bus) Setup(ctx context.Context) error {
	b.mu.RLock()
	transports := make(map[string]Transport, len(b.transports))
	for k, v := range b.transports {
		transports[k] = v
	}
	b.mu.RUnlock()

	for name, t := range transports {
		if setup, ok := t.(SetupTransport); ok {
			if err := setup.Setup(ctx); err != nil {
				return fmt.Errorf("queue: failed to setup transport %q: %w", name, err)
			}
		}
	}
	return nil
}

func (b *Bus) allHandlers() map[string]*handlerRegistration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make(map[string]*handlerRegistration, len(b.handlers))
	for k, v := range b.handlers {
		result[k] = v
	}
	return result
}
