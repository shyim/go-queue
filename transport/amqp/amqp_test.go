package amqp_test

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	queue "github.com/shyim/go-queue"
	queueamqp "github.com/shyim/go-queue/transport/amqp"
)

type testEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type testOrder struct {
	OrderID string  `json:"order_id"`
	Total   float64 `json:"total"`
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

func startRabbitMQ(t *testing.T) string {
	t.Helper()
	skipWithoutDocker(t)
	ctx := context.Background()

	container, err := tcrabbit.Run(ctx, "rabbitmq:4-alpine")
	if err != nil {
		t.Fatalf("failed to start rabbitmq: %s", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get amqp url: %s", err)
	}
	return url
}

func newTestTransport(t *testing.T, dsn string) *queueamqp.Transport {
	t.Helper()
	tr := queueamqp.NewTransport(queueamqp.Config{
		DSN:               dsn,
		Exchange:          "test-exchange",
		Queue:             "test-queue",
		RoutingKey:        "test",
		Durable:           false,
		AutoSetup:         true,
		PublisherConfirms: true,
		PrefetchCount:     5,
	})
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestSetup(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)

	if err := tr.Setup(context.Background()); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}
}

func TestSendAndReceive(t *testing.T) {
	dsn := startRabbitMQ(t)
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
		Headers: map[string]string{"custom": "header-value"},
	}
	if err := tr.Send(ctx, envelope); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	deliveries, err := tr.Receive(ctx)
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
		if received.Headers["custom"] != "header-value" {
			t.Errorf("got header custom=%q, want %q", received.Headers["custom"], "header-value")
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
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestAckAndNack(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	body, _ := json.Marshal(testEmail{To: "a@b.com", Subject: "test"})
	envelope := &queue.Envelope{
		Body:    body,
		Type:    "test.Email",
		ID:      "msg-nack",
		Headers: map[string]string{},
	}
	if err := tr.Send(ctx, envelope); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	deliveries, err := tr.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	select {
	case received := <-deliveries:
		if err := tr.Nack(ctx, received, true); err != nil {
			t.Fatalf("Nack failed: %s", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	select {
	case received := <-deliveries:
		if received.ID != "msg-nack" {
			t.Errorf("got id %q, want %q", received.ID, "msg-nack")
		}
		if err := tr.Ack(ctx, received); err != nil {
			t.Fatalf("Ack failed: %s", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for redelivery")
	}
}

func TestMultipleMessages(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	for i := range 10 {
		body, _ := json.Marshal(testOrder{OrderID: "order-" + string(rune('A'+i)), Total: float64(i) * 9.99})
		env := &queue.Envelope{
			Body:    body,
			Type:    "test.Order",
			ID:      "batch-" + string(rune('0'+i)),
			Headers: map[string]string{},
		}
		if err := tr.Send(ctx, env); err != nil {
			t.Fatalf("Send[%d] failed: %s", i, err)
		}
	}

	deliveries, err := tr.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	received := 0
	timeout := time.After(10 * time.Second)
	for received < 10 {
		select {
		case env := <-deliveries:
			if env.Type != "test.Order" {
				t.Errorf("got type %q, want %q", env.Type, "test.Order")
			}
			if err := tr.Ack(ctx, env); err != nil {
				t.Fatalf("Ack failed: %s", err)
			}
			received++
		case <-timeout:
			t.Fatalf("timed out after receiving %d/10 messages", received)
		}
	}
}

func TestDelayStamp(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	if err := tr.Setup(ctx); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	body, _ := json.Marshal(testEmail{To: "delayed@example.com", Subject: "Delayed"})
	envelope := &queue.Envelope{
		Body:    body,
		Type:    "test.Email",
		ID:      "msg-delayed",
		Headers: map[string]string{},
	}
	envelope.AddStamp(queue.DelayStamp{Delay: 500 * time.Millisecond})

	if err := tr.Send(ctx, envelope); err != nil {
		t.Fatalf("Send failed: %s", err)
	}

	deliveries, err := tr.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive failed: %s", err)
	}

	select {
	case received := <-deliveries:
		if received.ID != "msg-delayed" {
			t.Errorf("got id %q, want %q", received.ID, "msg-delayed")
		}
		_ = tr.Ack(ctx, received)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delayed message")
	}
}

func TestIntegrationWithBusAndWorker(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)
	ctx := context.Background()

	bus := queue.NewBus()
	bus.AddTransport("rabbitmq", tr)

	var handled atomic.Int32

	queue.HandleFunc[testEmail](bus, "rabbitmq", func(ctx context.Context, msg testEmail) error {
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

	workerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
			t.Fatal("timed out waiting for handler to be called")
		case <-time.After(50 * time.Millisecond):
		}
	}

	<-done

	if n := handled.Load(); n != 1 {
		t.Errorf("handler called %d times, want 1", n)
	}
}

func TestClose(t *testing.T) {
	dsn := startRabbitMQ(t)
	tr := newTestTransport(t, dsn)

	if err := tr.Setup(context.Background()); err != nil {
		t.Fatalf("Setup failed: %s", err)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close failed: %s", err)
	}

	body, _ := json.Marshal(testEmail{To: "x", Subject: "y"})
	err := tr.Send(context.Background(), &queue.Envelope{
		Body:    body,
		Type:    "test",
		ID:      "after-close",
		Headers: map[string]string{},
	})
	if err == nil {
		t.Fatal("expected error sending after Close")
	}
}
