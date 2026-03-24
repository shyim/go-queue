package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/shyim/go-queue"
)

const meterName = "github.com/shyim/go-queue/middleware/otel"

type metrics struct {
	dispatched metric.Int64Counter
	processed  metric.Int64Counter
	failed     metric.Int64Counter
	retried    metric.Int64Counter
	duration   metric.Float64Histogram
}

func newMetrics(m metric.Meter) *metrics {
	dispatched, _ := m.Int64Counter("messaging.client.sent.messages",
		metric.WithDescription("Number of messages dispatched"),
		metric.WithUnit("{message}"),
	)
	processed, _ := m.Int64Counter("messaging.client.consumed.messages",
		metric.WithDescription("Number of messages successfully processed"),
		metric.WithUnit("{message}"),
	)
	failed, _ := m.Int64Counter("messaging.client.consumed.messages.error",
		metric.WithDescription("Number of messages that failed permanently"),
		metric.WithUnit("{message}"),
	)
	retried, _ := m.Int64Counter("messaging.client.consumed.messages.retry",
		metric.WithDescription("Number of message retry attempts"),
		metric.WithUnit("{message}"),
	)
	duration, _ := m.Float64Histogram("messaging.process.duration",
		metric.WithDescription("Duration of message processing"),
		metric.WithUnit("s"),
	)
	return &metrics{
		dispatched: dispatched,
		processed:  processed,
		failed:     failed,
		retried:    retried,
		duration:   duration,
	}
}

// MetricsMiddleware returns a queue.Middleware that records OTel metrics
// for message processing: processed count, failed count, and duration histogram.
func MetricsMiddleware() queue.Middleware {
	return MetricsMiddlewareWithMeter(otel.Meter(meterName))
}

// MetricsMiddlewareWithMeter is like MetricsMiddleware but uses a specific meter.
func MetricsMiddlewareWithMeter(m metric.Meter) queue.Middleware {
	met := newMetrics(m)
	return func(ctx context.Context, envelope *queue.Envelope, next func(context.Context, *queue.Envelope) error) error {
		attrs := metric.WithAttributes(
			attribute.String("messaging.message.type", envelope.Type),
			attribute.String("messaging.destination.name", envelope.Transport),
		)

		start := time.Now()
		err := next(ctx, envelope)
		elapsed := time.Since(start).Seconds()

		met.duration.Record(ctx, elapsed, attrs)

		if err != nil {
			if queue.IsUnrecoverable(err) {
				met.failed.Add(ctx, 1, attrs)
			} else {
				met.retried.Add(ctx, 1, attrs)
			}
		} else {
			met.processed.Add(ctx, 1, attrs)
		}

		return err
	}
}

var dispatchCounter metric.Int64Counter

func getDispatchCounter() metric.Int64Counter {
	if dispatchCounter == nil {
		dispatchCounter, _ = otel.Meter(meterName).Int64Counter("messaging.client.sent.messages",
			metric.WithDescription("Number of messages dispatched"),
			metric.WithUnit("{message}"),
		)
	}
	return dispatchCounter
}

// RecordDispatch records a dispatched message metric.
func RecordDispatch(ctx context.Context, msgType string) {
	getDispatchCounter().Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.message.type", msgType),
	))
}
