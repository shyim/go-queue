package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/shyim/go-queue"
	queueotel "github.com/shyim/go-queue/middleware/otel"
	"github.com/shyim/go-queue/transport/memory"
)

func setupMeter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func sumValue(t *testing.T, m metricdata.Metrics) int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func TestMetricsMiddlewareCountsProcessed(t *testing.T) {
	reader := setupMeter(t)
	setupTracer(t)

	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		close(done)
		return nil
	})

	_ = queue.Dispatch(context.Background(), bus, testMsg{Value: "ok"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{queueotel.MetricsMiddleware()},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	m := collectMetric(t, reader, "messaging.client.consumed.messages")
	if v := sumValue(t, m); v != 1 {
		t.Errorf("expected 1 processed, got %d", v)
	}
}

func TestMetricsMiddlewareCountsFailed(t *testing.T) {
	reader := setupMeter(t)
	setupTracer(t)

	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		defer func() {
			select {
			case <-done:
			default:
				close(done)
			}
		}()
		return queue.Unrecoverable(errors.New("fatal"))
	})

	_ = queue.Dispatch(context.Background(), bus, testMsg{Value: "fail"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{queueotel.MetricsMiddleware()},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	m := collectMetric(t, reader, "messaging.client.consumed.messages.error")
	if v := sumValue(t, m); v != 1 {
		t.Errorf("expected 1 failed, got %d", v)
	}
}

func TestMetricsMiddlewareRecordsDuration(t *testing.T) {
	reader := setupMeter(t)
	setupTracer(t)

	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	done := make(chan struct{})
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		time.Sleep(10 * time.Millisecond)
		close(done)
		return nil
	})

	_ = queue.Dispatch(context.Background(), bus, testMsg{Value: "slow"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{
		Concurrency: 1,
		Middleware:  []queue.Middleware{queueotel.MetricsMiddleware()},
	})
	go func() { _ = worker.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	m := collectMetric(t, reader, "messaging.process.duration")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram[float64], got %T", m.Data)
	}
	if len(hist.DataPoints) == 0 {
		t.Fatal("expected at least 1 histogram data point")
	}
	if hist.DataPoints[0].Count != 1 {
		t.Errorf("expected count=1, got %d", hist.DataPoints[0].Count)
	}
	if hist.DataPoints[0].Sum < 0.01 {
		t.Errorf("expected duration >= 10ms, got %fs", hist.DataPoints[0].Sum)
	}
}

func TestDispatchRecordsMetric(t *testing.T) {
	reader := setupMeter(t)
	setupTracer(t)

	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)
	bus.AddDispatchMiddleware(queueotel.DispatchMiddleware())
	queue.HandleFunc[testMsg](bus, "async", func(ctx context.Context, msg testMsg) error {
		return nil
	})

	err := queue.Dispatch(context.Background(), bus, testMsg{Value: "counted"})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	m := collectMetric(t, reader, "messaging.client.sent.messages")
	if v := sumValue(t, m); v != 1 {
		t.Errorf("expected 1 dispatched, got %d", v)
	}
}
