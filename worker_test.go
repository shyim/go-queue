package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shyim/go-queue"
	"github.com/shyim/go-queue/transport/memory"
)

type workerTestMsg struct {
	Value string `json:"value"`
}

func TestWorkerProcessesMessages(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var processed atomic.Value
	done := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		processed.Store(msg.Value)
		close(done)
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "hello"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{Concurrency: 1})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for message to be processed")
	}

	if v := processed.Load(); v != "hello" {
		t.Errorf("expected processed value %q, got %v", "hello", v)
	}

	cancel()

	time.Sleep(50 * time.Millisecond)
	acked := transport.Acked()
	if len(acked) != 1 {
		t.Errorf("expected 1 acked envelope, got %d", len(acked))
	}
}

func TestWorkerRetriesOnError(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var attempts atomic.Int32
	done := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("transient error (attempt %d)", n)
		}
		close(done)
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "retry-me"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		RetryStrategy: queue.RetryStrategy{
			MaxRetries: 5,
			Delay:      1 * time.Millisecond,
			Multiplier: 1.0,
			MaxDelay:   10 * time.Millisecond,
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for retried message")
	}

	if n := attempts.Load(); n != 3 {
		t.Errorf("expected 3 attempts, got %d", n)
	}

	cancel()
}

func TestWorkerDoesNotRetryUnrecoverable(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var attempts atomic.Int32
	done := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		attempts.Add(1)
		defer func() { close(done) }()
		return queue.Unrecoverable(errors.New("permanent failure"))
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "fail"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		RetryStrategy: queue.RetryStrategy{
			MaxRetries: 5,
			Delay:      1 * time.Millisecond,
			Multiplier: 1.0,
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	if n := attempts.Load(); n != 1 {
		t.Errorf("expected exactly 1 attempt for unrecoverable error, got %d", n)
	}

	nacked := transport.Nacked()
	if len(nacked) != 1 {
		t.Errorf("expected 1 nacked envelope, got %d", len(nacked))
	}
}

func TestWorkerRespectsMaxRetries(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var attempts atomic.Int32
	allDone := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		n := attempts.Add(1)
		if n >= 3 {
			defer func() {
				select {
				case <-allDone:
				default:
					close(allDone)
				}
			}()
		}
		return fmt.Errorf("always fail (attempt %d)", n)
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "max-retry"},
		queue.WithMaxRetries(2),
	)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		RetryStrategy: queue.RetryStrategy{
			MaxRetries: 10,
			Delay:      1 * time.Millisecond,
			Multiplier: 1.0,
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-allDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for max retries")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	if n := attempts.Load(); n != 3 {
		t.Errorf("expected 3 attempts (original + 2 retries), got %d", n)
	}
}

func TestWorkerGracefulShutdown(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	worker := queue.NewWorker(bus, queue.WorkerConfig{Concurrency: 1})

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not shut down in time")
	}
}

func TestWorkerErrorHandlerCallback(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	handlerErr := errors.New("handler error")

	done := make(chan struct{})
	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		defer func() {
			select {
			case <-done:
			default:
				close(done)
			}
		}()
		return queue.Unrecoverable(handlerErr)
	})

	var mu sync.Mutex
	var capturedErr error

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "error-callback"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		ErrorHandler: func(ctx context.Context, envelope *queue.Envelope, err error) {
			mu.Lock()
			defer mu.Unlock()
			capturedErr = err
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	if capturedErr == nil {
		t.Fatal("expected error handler to be called")
	}
	if !errors.Is(capturedErr, handlerErr) {
		t.Errorf("expected error to wrap %v, got %v", handlerErr, capturedErr)
	}
}

func TestWorkerFailureHandler(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		defer func() {
			select {
			case <-done:
			default:
				close(done)
			}
		}()
		return queue.Unrecoverable(errors.New("fatal"))
	})

	var mu sync.Mutex
	var failedErr error

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "fail-me"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		FailureHandler: func(ctx context.Context, envelope *queue.Envelope, err error) {
			mu.Lock()
			defer mu.Unlock()
			failedErr = err
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	if failedErr == nil {
		t.Fatal("expected failure handler to be called")
	}
}

func TestWorkerFailureHandlerOnMaxRetries(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var attempts atomic.Int32
	allDone := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		n := attempts.Add(1)
		if n >= 2 {
			defer func() {
				select {
				case <-allDone:
				default:
					close(allDone)
				}
			}()
		}
		return fmt.Errorf("always fail (attempt %d)", n)
	})

	var mu sync.Mutex
	var failureCalled bool

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "exhaust-retries"},
		queue.WithMaxRetries(1),
	)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		RetryStrategy: queue.RetryStrategy{
			MaxRetries: 10,
			Delay:      1 * time.Millisecond,
			Multiplier: 1.0,
		},
		FailureHandler: func(ctx context.Context, envelope *queue.Envelope, err error) {
			mu.Lock()
			defer mu.Unlock()
			failureCalled = true
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-allDone:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	if !failureCalled {
		t.Fatal("expected failure handler to be called after max retries")
	}
}

func TestWorkerPanicRecovery(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		defer func() {
			select {
			case <-done:
			default:
				close(done)
			}
		}()
		panic("handler exploded")
	})

	var mu sync.Mutex
	var failureCalled bool

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "panic"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		FailureHandler: func(ctx context.Context, envelope *queue.Envelope, err error) {
			mu.Lock()
			defer mu.Unlock()
			failureCalled = true
		},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	if !failureCalled {
		t.Fatal("expected failure handler to be called on panic")
	}
}

func TestWorkerHandlerTimeout(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		defer func() {
			select {
			case <-done:
			default:
				close(done)
			}
		}()
		<-ctx.Done()
		return ctx.Err()
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "slow"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency:    1,
		HandlerTimeout: 50 * time.Millisecond,
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out — handler timeout didn't fire")
	}

	cancel()
}

func TestWorkerMiddleware(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	var order []string
	var mu sync.Mutex
	done := make(chan struct{})

	queue.HandleFunc[workerTestMsg](bus, "async", func(ctx context.Context, msg workerTestMsg) error {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
		close(done)
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, workerTestMsg{Value: "mw"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	mw1 := func(ctx context.Context, env *queue.Envelope, next func(context.Context, *queue.Envelope) error) error {
		mu.Lock()
		order = append(order, "before-1")
		mu.Unlock()
		err := next(ctx, env)
		mu.Lock()
		order = append(order, "after-1")
		mu.Unlock()
		return err
	}
	mw2 := func(ctx context.Context, env *queue.Envelope, next func(context.Context, *queue.Envelope) error) error {
		mu.Lock()
		order = append(order, "before-2")
		mu.Unlock()
		err := next(ctx, env)
		mu.Lock()
		order = append(order, "after-2")
		mu.Unlock()
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{mw1, mw2},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	mu.Lock()
	defer mu.Unlock()

	expected := []string{"before-1", "before-2", "handler", "after-2", "after-1"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d calls, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %q, want %q", i, order[i], v)
		}
	}
}
