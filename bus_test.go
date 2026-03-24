package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/shyim/go-queue"
	"github.com/shyim/go-queue/transport/memory"
)

type testEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type unknownMessage struct {
	Data string `json:"data"`
}

func TestDispatchWithRegisteredHandler(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	queue.HandleFunc[testEmail](bus, "async", func(ctx context.Context, msg testEmail) error {
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testEmail{To: "a@b.com", Subject: "Hi"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	select {
	case env := <-transport.Chan():
		if env.Transport != "async" {
			t.Errorf("expected transport %q, got %q", "async", env.Transport)
		}
		if env.ID == "" {
			t.Error("expected non-empty envelope ID")
		}
		stamp, ok := queue.GetStamp[queue.SentStamp](env)
		if !ok {
			t.Error("expected SentStamp")
		}
		if stamp.TransportName != "async" {
			t.Errorf("expected SentStamp transport %q, got %q", "async", stamp.TransportName)
		}
	default:
		t.Fatal("expected an envelope on the transport channel")
	}
}

func TestDispatchUnknownMessageType(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	err := queue.Dispatch(context.Background(), bus, unknownMessage{Data: "test"})
	if err == nil {
		t.Fatal("expected error for unknown message type")
	}
}

func TestDispatchWithOptions(t *testing.T) {
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	queue.HandleFunc[testEmail](bus, "async", func(ctx context.Context, msg testEmail) error {
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testEmail{To: "a@b.com", Subject: "Hi"},
		queue.WithDelay(5*time.Second),
		queue.WithMaxRetries(10),
		queue.WithHeader("x-priority", "high"),
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	select {
	case env := <-transport.Chan():
		delayStamp, ok := queue.GetStamp[queue.DelayStamp](env)
		if !ok {
			t.Fatal("expected DelayStamp")
		}
		if delayStamp.Delay != 5*time.Second {
			t.Errorf("expected delay 5s, got %v", delayStamp.Delay)
		}

		redelivery, ok := queue.GetStamp[queue.RedeliveryStamp](env)
		if !ok {
			t.Fatal("expected RedeliveryStamp")
		}
		if redelivery.MaxRetries != 10 {
			t.Errorf("expected max retries 10, got %d", redelivery.MaxRetries)
		}

		if env.Headers["x-priority"] != "high" {
			t.Errorf("expected header x-priority=high, got %q", env.Headers["x-priority"])
		}
	default:
		t.Fatal("expected an envelope on the transport channel")
	}
}
