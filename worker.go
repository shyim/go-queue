package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"
)

// WorkerConfig configures the [Worker].
type WorkerConfig struct {
	Concurrency    int
	RetryStrategy  RetryStrategy
	HandlerTimeout time.Duration
	// ShutdownTimeout is how long to wait for in-flight handlers to finish
	// after the context is cancelled. Defaults to 30s.
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
	ErrorHandler    func(ctx context.Context, envelope *Envelope, err error)
	// FailureHandler is called when a message is permanently rejected
	// (max retries exceeded or unrecoverable error). Use this to route
	// failed messages to a dead-letter store, alert, or log for replay.
	// If nil, the message is silently discarded.
	FailureHandler func(ctx context.Context, envelope *Envelope, err error)
	Middleware     []Middleware
}

// Worker consumes messages from transports and dispatches them to handlers.
type Worker struct {
	bus        *Bus
	config     WorkerConfig
	logger     *slog.Logger
	middleware []Middleware
}

// NewWorker creates a new worker for the given bus.
func NewWorker(bus *Bus, config WorkerConfig) *Worker {
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.RetryStrategy == (RetryStrategy{}) {
		config.RetryStrategy = DefaultRetryStrategy()
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		bus:        bus,
		config:     config,
		logger:     logger,
		middleware: config.Middleware,
	}
}

// Run starts the worker and blocks until the context is cancelled.
// On cancellation it waits for in-flight messages to finish processing.
func (w *Worker) Run(ctx context.Context) error {
	transportsToConsume := make(map[string]Transport)
	for typeName, reg := range w.bus.allHandlers() {
		t, ok := w.bus.getTransport(reg.transport)
		if !ok {
			return fmt.Errorf("queue: transport %q not found for message type %s", reg.transport, typeName)
		}
		transportsToConsume[reg.transport] = t
	}

	if len(transportsToConsume) == 0 {
		return fmt.Errorf("queue: no transports to consume from")
	}

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()

	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for name, transport := range transportsToConsume {
		deliveries, err := transport.Receive(consumeCtx)
		if err != nil {
			consumeCancel()
			return fmt.Errorf("queue: failed to start consuming from %q: %w", name, err)
		}

		w.logger.Info("consuming from transport", "transport", name, "concurrency", w.config.Concurrency)

		for i := range w.config.Concurrency {
			wg.Add(1)
			go func(workerID int, tName string, t Transport, ch <-chan *Envelope) {
				defer wg.Done()
				w.logger.Debug("worker started", "worker_id", workerID, "transport", tName)
				for envelope := range ch {
					w.processEnvelope(processCtx, tName, t, envelope)
				}
				w.logger.Debug("worker stopped", "worker_id", workerID, "transport", tName)
			}(i, name, transport, deliveries)
		}
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		w.logger.Info("shutting down worker, waiting for in-flight messages")
		consumeCancel()

		timer := time.NewTimer(w.config.ShutdownTimeout)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			w.logger.Warn("shutdown timeout exceeded, cancelling in-flight handlers")
			processCancel()
			wg.Wait()
		}

		w.logger.Info("worker stopped")
		return ctx.Err()
	}
}

func (w *Worker) processEnvelope(ctx context.Context, transportName string, t Transport, envelope *Envelope) {
	envelope.AddStamp(ReceivedStamp{
		TransportName: transportName,
		ReceivedAt:    time.Now(),
	})
	ctx = WithEnvelope(ctx, envelope)

	reg, ok := w.bus.GetHandler(envelope.Type)
	if !ok {
		w.logger.Error("no handler for message type", "type", envelope.Type)
		_ = t.Nack(ctx, envelope, false)
		return
	}

	err := w.invokeHandler(ctx, reg, envelope)
	if err != nil {
		w.handleError(ctx, t, envelope, err)
		return
	}

	if ackErr := t.Ack(ctx, envelope); ackErr != nil {
		w.logger.Error("failed to ack message", "error", ackErr, "type", envelope.Type, "id", envelope.ID)
	}

	envelope.AddStamp(HandledStamp{
		HandledAt: time.Now(),
	})
}

func (w *Worker) invokeHandler(ctx context.Context, reg *handlerRegistration, envelope *Envelope) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = Unrecoverable(fmt.Errorf("queue: handler panicked: %v", r))
		}
	}()

	msgPtr := reflect.New(reg.msgType)
	if err := json.Unmarshal(envelope.Body, msgPtr.Interface()); err != nil {
		return Unrecoverable(fmt.Errorf("queue: failed to unmarshal message: %w", err))
	}
	msg := msgPtr.Elem().Interface()

	handlerCtx := ctx
	var cancel context.CancelFunc
	if w.config.HandlerTimeout > 0 {
		handlerCtx, cancel = context.WithTimeout(ctx, w.config.HandlerTimeout)
		defer cancel()
	}

	call := func(callCtx context.Context, _ *Envelope) error {
		handlerValue := reflect.ValueOf(reg.handler)
		method := handlerValue.MethodByName("Handle")
		results := method.Call([]reflect.Value{
			reflect.ValueOf(callCtx),
			reflect.ValueOf(msg),
		})
		if !results[0].IsNil() {
			return results[0].Interface().(error)
		}
		return nil
	}

	chain := call
	for i := len(w.middleware) - 1; i >= 0; i-- {
		mw := w.middleware[i]
		next := chain
		chain = func(callCtx context.Context, env *Envelope) error {
			return mw(callCtx, env, next)
		}
	}

	return chain(handlerCtx, envelope)
}

func (w *Worker) handleError(ctx context.Context, t Transport, envelope *Envelope, err error) {
	w.logger.Error("handler failed", "error", err, "type", envelope.Type, "id", envelope.ID)

	if w.config.ErrorHandler != nil {
		w.config.ErrorHandler(ctx, envelope, err)
	}

	if IsUnrecoverable(err) {
		w.logger.Warn("unrecoverable error, rejecting message", "type", envelope.Type, "id", envelope.ID)
		w.rejectMessage(ctx, t, envelope, err)
		return
	}

	redelivery, _ := GetLastStamp[RedeliveryStamp](envelope)
	maxRetries := w.config.RetryStrategy.MaxRetries
	if redelivery.MaxRetries > 0 {
		maxRetries = redelivery.MaxRetries
	}

	if redelivery.Attempt >= maxRetries {
		w.logger.Warn("max retries exceeded, rejecting message",
			"type", envelope.Type, "id", envelope.ID, "attempts", redelivery.Attempt)
		w.rejectMessage(ctx, t, envelope, err)
		return
	}

	delay := w.calculateRetryDelay(redelivery.Attempt)
	w.logger.Info("retrying message",
		"type", envelope.Type, "id", envelope.ID,
		"attempt", redelivery.Attempt+1, "delay", delay)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		_ = t.Nack(ctx, envelope, true)
		return
	}

	envelope.AddStamp(RedeliveryStamp{
		Attempt:    redelivery.Attempt + 1,
		MaxRetries: maxRetries,
		LastError:  err.Error(),
	})

	if retryErr := t.Retry(ctx, envelope); retryErr != nil {
		w.logger.Error("failed to retry message, nacking", "error", retryErr, "type", envelope.Type, "id", envelope.ID)
		_ = t.Nack(ctx, envelope, false)
	}
}

func (w *Worker) rejectMessage(ctx context.Context, t Transport, envelope *Envelope, err error) {
	if w.config.FailureHandler != nil {
		w.config.FailureHandler(ctx, envelope, err)
	}
	_ = t.Nack(ctx, envelope, false)
}

func (w *Worker) calculateRetryDelay(attempt int) time.Duration {
	s := w.config.RetryStrategy
	delay := float64(s.Delay) * math.Pow(s.Multiplier, float64(attempt))

	if s.MaxDelay > 0 && time.Duration(delay) > s.MaxDelay {
		delay = float64(s.MaxDelay)
	}

	if s.Jitter > 0 {
		jitter := delay * s.Jitter
		delay += (rand.Float64()*2 - 1) * jitter
	}

	return time.Duration(delay)
}
