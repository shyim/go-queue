package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	queue "github.com/shyim/go-queue"
	queuepg "github.com/shyim/go-queue/transport/postgres"
)

type testEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func skipWithoutDocker(t *testing.T) {
	t.Helper()
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return
	}
	if _, err := os.Stat("/var/run/docker.sock"); err == nil {
		return
	}
	t.Skip("docker not available (set DOCKER_HOST), skipping integration test")
}

func startPostgres(t *testing.T) string {
	t.Helper()
	skipWithoutDocker(t)
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres: %s", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}
	return dsn
}

func newTestTransport(t *testing.T, dsn string) *queuepg.Transport {
	t.Helper()
	tr := queuepg.NewTransport(queuepg.Config{
		DSN:               dsn,
		Table:             "test_queue",
		Channel:           "test_notify",
		PollInterval:      500 * time.Millisecond,
		ReaperInterval:    1 * time.Second,
		VisibilityTimeout: 5 * time.Second,
	})
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestSetup(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)

	if err := tr.Setup(context.Background()); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}
}

func TestConcurrentSetup(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)

	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			tr := queuepg.NewTransport(queuepg.Config{
				DSN:     dsn,
				Table:   "concurrent_queue",
				Channel: "concurrent_notify",
			})
			defer tr.Close()
			if err := tr.Setup(ctx); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Setup failed: %s", err)
	}
}

func TestSendAndReceive(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	msg := testEmail{To: "user@example.com", Subject: "Hello"}
	body, _ := json.Marshal(msg)
	envelope := &queue.Envelope{
		Body:    body,
		Type:    "test.Email",
		ID:      "msg-001",
		Headers: map[string]string{"custom": "value"},
	}
	if err := tr.Send(ctx, envelope); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	deliveries, err := tr.Receive(recvCtx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	select {
	case received := <-deliveries:
		if received.Type != "test.Email" {
			t.Errorf("got type %q, want %q", received.Type, "test.Email")
		}
		if received.ID != "msg-001" {
			t.Errorf("got id %q, want %q", received.ID, "msg-001")
		}
		if received.Headers["custom"] != "value" {
			t.Errorf("got header custom=%q, want %q", received.Headers["custom"], "value")
		}
		var got testEmail
		if err := json.Unmarshal(received.Body, &got); err != nil {
			t.Fatalf("unmarshal: %s", err)
		}
		if got != msg {
			t.Errorf("got %+v, want %+v", got, msg)
		}
		if err := tr.Ack(ctx, received); err != nil {
			t.Fatalf("Ack failed: %s", err)
		}
	case <-recvCtx.Done():
		t.Fatal("timed out waiting for message")
	}

	cancel()
}

func TestNackRequeue(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	body, _ := json.Marshal(testEmail{To: "a@b.com", Subject: "test"})
	if err := tr.Send(ctx, &queue.Envelope{
		Body: body, Type: "test.Email", ID: "msg-nack", Headers: map[string]string{},
	}); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	deliveries, err := tr.Receive(recvCtx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	// First delivery — nack with requeue.
	select {
	case received := <-deliveries:
		if err := tr.Nack(ctx, received, true); err != nil {
			t.Fatalf("Nack failed: %s", err)
		}
	case <-recvCtx.Done():
		t.Fatal("timed out")
	}

	// Should be redelivered.
	select {
	case received := <-deliveries:
		if received.ID != "msg-nack" {
			t.Errorf("got id %q, want %q", received.ID, "msg-nack")
		}
		_ = tr.Ack(ctx, received)
	case <-recvCtx.Done():
		t.Fatal("timed out waiting for redelivery")
	}

	cancel()
}

func TestMultipleMessages(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	for i := range 5 {
		body, _ := json.Marshal(testEmail{To: "batch@test.com", Subject: fmt.Sprintf("msg-%d", i)})
		if err := tr.Send(ctx, &queue.Envelope{
			Body: body, Type: "test.Email", ID: fmt.Sprintf("batch-%d", i), Headers: map[string]string{},
		}); err != nil {
			t.Fatalf("Send[%d] failed: %s", i, err)
		}
	}

	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	deliveries, err := tr.Receive(recvCtx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	received := 0
	for received < 5 {
		select {
		case env := <-deliveries:
			_ = tr.Ack(ctx, env)
			received++
		case <-recvCtx.Done():
			t.Fatalf("timed out after receiving %d/5 messages", received)
		}
	}

	cancel()
}

func TestIntegrationWithBusAndWorker(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	bus := queue.NewBus()
	bus.AddTransport("pg", tr)

	var handled atomic.Int32
	queue.HandleFunc[testEmail](bus, "pg", func(ctx context.Context, msg testEmail) error {
		handled.Add(1)
		if msg.To != "integration@test.com" {
			t.Errorf("got To=%q, want %q", msg.To, "integration@test.com")
		}
		return nil
	})

	if err := bus.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	if err := queue.Dispatch(ctx, bus, testEmail{
		To:      "integration@test.com",
		Subject: "Integration Test",
	}); err != nil {
		t.Fatalf("Dispatch failed: %s", err)
	}

	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{Concurrency: 1})

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(workerCtx)
	}()

	deadline := time.After(10 * time.Second)
	for {
		if handled.Load() >= 1 {
			cancel()
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("timed out waiting for handler")
		case <-time.After(50 * time.Millisecond):
		}
	}

	<-done

	if n := handled.Load(); n != 1 {
		t.Errorf("handler called %d times, want 1", n)
	}
}

func TestDelayedMessage(t *testing.T) {
	dsn := startPostgres(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	body, _ := json.Marshal(testEmail{To: "delayed@test.com", Subject: "delayed"})
	envelope := &queue.Envelope{
		Body: body, Type: "test.Email", ID: "msg-delayed", Headers: map[string]string{},
	}
	envelope.AddStamp(queue.DelayStamp{Delay: 1 * time.Second})

	start := time.Now()
	if err := tr.Send(ctx, envelope); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	deliveries, err := tr.Receive(recvCtx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	select {
	case received := <-deliveries:
		elapsed := time.Since(start)
		if elapsed < 900*time.Millisecond {
			t.Errorf("message received too early: %v", elapsed)
		}
		_ = tr.Ack(ctx, received)
	case <-recvCtx.Done():
		t.Fatal("timed out waiting for delayed message")
	}

	cancel()
}
