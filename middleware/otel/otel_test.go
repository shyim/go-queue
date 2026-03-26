package otel_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/shyim/go-queue"
	queueotel "github.com/shyim/go-queue/middleware/otel"
	"github.com/shyim/go-queue/transport/memory"
)

type testMsg struct {
	Value string `json:"value"`
}

func setupTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
	})
	return exporter
}

func newBusWithOtel(t *testing.T) (*queue.Bus, *memory.Transport) {
	t.Helper()
	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)
	bus.AddDispatchMiddleware(queueotel.DispatchMiddleware())
	return bus, transport
}

func TestDispatchMiddlewareCreatesProducerSpan(t *testing.T) {
	exporter := setupTracer(t)
	bus, _ := newBusWithOtel(t)

	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testMsg{Value: "traced"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].SpanKind != trace.SpanKindProducer {
		t.Errorf("expected SpanKindProducer, got %v", spans[0].SpanKind)
	}
}

func TestDispatchMiddlewareInjectsTraceContext(t *testing.T) {
	setupTracer(t)
	bus, transport := newBusWithOtel(t)

	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testMsg{Value: "headers"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case env := <-transport.Chan():
		if env.Headers["traceparent"] == "" {
			t.Error("expected traceparent header to be set")
		}
	default:
		t.Fatal("expected envelope")
	}
}

func TestConsumerMiddlewareCreatesSpan(t *testing.T) {
	exporter := setupTracer(t)
	bus, _ := newBusWithOtel(t)

	var handled atomic.Bool
	done := make(chan struct{})
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		span := trace.SpanFromContext(ctx)
		if !span.SpanContext().IsValid() {
			t.Error("expected valid span context in handler")
		}
		handled.Store(true)
		close(done)
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testMsg{Value: "consume"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{queueotel.Middleware()},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	if !handled.Load() {
		t.Fatal("handler not called")
	}

	spans := exporter.GetSpans()
	var hasProducer, hasConsumer bool
	for _, s := range spans {
		switch s.SpanKind {
		case trace.SpanKindProducer:
			hasProducer = true
		case trace.SpanKindConsumer:
			hasConsumer = true
		}
	}
	if !hasProducer {
		t.Error("expected a producer span")
	}
	if !hasConsumer {
		t.Error("expected a consumer span")
	}
}

func TestTraceLinkedAcrossDispatchAndConsume(t *testing.T) {
	exporter := setupTracer(t)
	bus, _ := newBusWithOtel(t)

	done := make(chan struct{})
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		close(done)
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testMsg{Value: "linked"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{queueotel.Middleware()},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	spans := exporter.GetSpans()
	var producerTraceID, consumerTraceID trace.TraceID
	for _, s := range spans {
		switch s.SpanKind {
		case trace.SpanKindProducer:
			producerTraceID = s.SpanContext.TraceID()
		case trace.SpanKindConsumer:
			consumerTraceID = s.SpanContext.TraceID()
		}
	}

	if producerTraceID != consumerTraceID {
		t.Errorf("trace IDs don't match: producer=%s consumer=%s", producerTraceID, consumerTraceID)
	}
}

func TestSpanNameNormalizerChangesSpanNameOnly(t *testing.T) {
	exporter := setupTracer(t)

	env := &queue.Envelope{
		Type:      "github.com/friendsofshopware/shopmon/api/internal/jobs.ShopScrape",
		Transport: "async",
		Body:      []byte(`{"value":"normalized"}`),
		Headers:   map[string]string{},
	}

	err := queueotel.DispatchMiddleware(
		queueotel.WithSpanNameNormalizer(queueotel.DefaultSpanNameNormalizer),
	)(context.Background(), env, func(ctx context.Context, envelope *queue.Envelope) error {
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(envelope.Headers))
		return nil
	})
	if err != nil {
		t.Fatalf("producer middleware error: %v", err)
	}

	err = queueotel.Middleware(
		queueotel.WithSpanNameNormalizer(queueotel.DefaultSpanNameNormalizer),
	)(context.Background(), env, func(ctx context.Context, envelope *queue.Envelope) error {
		return nil
	})
	if err != nil {
		t.Fatalf("consumer middleware error: %v", err)
	}

	spans := exporter.GetSpans()
	var producerFound, consumerFound bool
	for _, s := range spans {
		switch s.Name {
		case "ShopScrape send":
			producerFound = true
		case "ShopScrape process":
			consumerFound = true
		}

		for _, attr := range s.Attributes {
			if attr.Key == "messaging.message.type" && attr.Value.AsString() != "github.com/friendsofshopware/shopmon/api/internal/jobs.ShopScrape" {
				t.Fatalf("expected messaging.message.type to keep original value, got %q", attr.Value.AsString())
			}
		}
	}

	if !producerFound {
		t.Fatal("expected normalized producer span name")
	}
	if !consumerFound {
		t.Fatal("expected normalized consumer span name")
	}
}
