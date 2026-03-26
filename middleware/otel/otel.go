// Package otel provides OpenTelemetry tracing and metrics middleware for go-queue.
//
// Usage:
//
//	import queueotel "github.com/shyim/go-queue/middleware/otel"
//
//	// Register dispatch + consumer middleware once:
//	bus.AddDispatchMiddleware(queueotel.DispatchMiddleware())
//
//	worker := queue.NewWorker(bus, queue.WorkerConfig{
//	    Middleware: []queue.Middleware{
//	        queueotel.Middleware(),
//	        queueotel.MetricsMiddleware(),
//	    },
//	})
//
//	// Then use queue.Dispatch as usual — tracing is automatic.
//	queue.Dispatch(ctx, bus, msg)
package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/shyim/go-queue"
)

const tracerName = "github.com/shyim/go-queue/middleware/otel"

// DispatchMiddleware returns a dispatch middleware that creates a producer span
// and injects trace context into envelope headers. Register it on the bus:
//
//	bus.AddDispatchMiddleware(queueotel.DispatchMiddleware())
func DispatchMiddleware(opts ...Option) queue.Middleware {
	return DispatchMiddlewareWithTracer(otel.Tracer(tracerName), opts...)
}

// DispatchMiddlewareWithTracer is like DispatchMiddleware but uses a specific tracer.
func DispatchMiddlewareWithTracer(tracer trace.Tracer, opts ...Option) queue.Middleware {
	cfg := newConfig(opts...)

	return func(ctx context.Context, envelope *queue.Envelope, next func(context.Context, *queue.Envelope) error) error {
		spanName := cfg.spanNameFormatter(envelope, "send")

		attrs := []attribute.KeyValue{
			semconv.MessagingOperationTypeSend,
			semconv.MessagingOperationName("send"),
		}
		if envelope.Type != "" {
			attrs = append(attrs, attribute.String("messaging.message.type", envelope.Type))
		}
		if envelope.ID != "" {
			attrs = append(attrs, semconv.MessagingMessageID(envelope.ID))
		}
		if envelope.Transport != "" {
			attrs = append(attrs, semconv.MessagingDestinationName(envelope.Transport))
		}

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		if envelope.Headers == nil {
			envelope.Headers = make(map[string]string)
		}
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(envelope.Headers))

		err := next(ctx, envelope)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
			RecordDispatch(ctx, envelope.Type)
		}
		return err
	}
}

// Middleware returns a consumer middleware that:
//   - Extracts trace context from envelope headers (linking to the producer span)
//   - Creates a consumer span with messaging semantic conventions
//   - Records errors and sets span status
func Middleware(opts ...Option) queue.Middleware {
	return MiddlewareWithTracer(otel.Tracer(tracerName), opts...)
}

// MiddlewareWithTracer is like Middleware but uses a specific tracer.
func MiddlewareWithTracer(tracer trace.Tracer, opts ...Option) queue.Middleware {
	cfg := newConfig(opts...)

	return func(ctx context.Context, envelope *queue.Envelope, next func(context.Context, *queue.Envelope) error) error {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(envelope.Headers))

		attrs := []attribute.KeyValue{
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingOperationName("process"),
			semconv.MessagingMessageBodySize(len(envelope.Body)),
		}
		if envelope.Type != "" {
			attrs = append(attrs, attribute.String("messaging.message.type", envelope.Type))
		}
		if envelope.ID != "" {
			attrs = append(attrs, semconv.MessagingMessageID(envelope.ID))
		}
		if envelope.Transport != "" {
			attrs = append(attrs, semconv.MessagingDestinationName(envelope.Transport))
		}

		spanName := cfg.spanNameFormatter(envelope, "process")

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		err := next(ctx, envelope)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}

		return err
	}
}
