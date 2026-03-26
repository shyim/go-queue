# OpenTelemetry Middleware

Optional OpenTelemetry tracing and metrics for go-queue. Import only if you need observability — no dependency on OTel in the core library.

```sh
go get github.com/shyim/go-queue
```

## Setup

Register middleware once — all `queue.Dispatch` and worker calls are traced automatically:

```go
import queueotel "github.com/shyim/go-queue/middleware/otel"

bus := queue.NewBus()
bus.AddTransport("async", transport)

// Producer tracing: creates spans and injects traceparent into headers.
bus.AddDispatchMiddleware(queueotel.DispatchMiddleware())

// Consumer tracing + metrics.
worker := queue.NewWorker(bus, queue.WorkerConfig{
	Middleware: []queue.Middleware{
		queueotel.Middleware(),        // tracing
		queueotel.MetricsMiddleware(), // metrics
	},
})

// Then use queue.Dispatch as usual — tracing is automatic.
queue.Dispatch(ctx, bus, MyMsg{...})
```

## Tracing

Producer and consumer spans are linked by trace context propagated through message headers.

### Span Attributes

Producer spans:
- `messaging.operation.type` = `send`
- `messaging.operation.name` = `send`
- `messaging.message.type` = Go type name
- `messaging.message.id` = envelope ID
- `messaging.destination.name` = transport name

Consumer spans:
- `messaging.operation.type` = `process`
- `messaging.operation.name` = `process`
- `messaging.message.id` = envelope ID
- `messaging.message.body.size` = body length
- `messaging.destination.name` = transport name
- `messaging.message.type` = Go type name

### Custom Tracer

```go
tracer := otel.Tracer("my-app")
bus.AddDispatchMiddleware(queueotel.DispatchMiddlewareWithTracer(tracer))

// Consumer side:
queueotel.MiddlewareWithTracer(tracer)
```

### Span Name Normalization

If your message type is too verbose for span names, you can normalize just the
span name while keeping `messaging.message.type` unchanged:

```go
normalize := func(messageType string) string {
	if idx := strings.LastIndex(messageType, "."); idx >= 0 {
		return messageType[idx+1:]
	}

	return messageType
}

bus.AddDispatchMiddleware(queueotel.DispatchMiddleware(
	queueotel.WithSpanNameNormalizer(normalize),
))

worker := queue.NewWorker(bus, queue.WorkerConfig{
	Middleware: []queue.Middleware{
		queueotel.Middleware(queueotel.WithSpanNameNormalizer(normalize)),
	},
})
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messaging.client.sent.messages` | Counter | Messages dispatched |
| `messaging.client.consumed.messages` | Counter | Messages successfully processed |
| `messaging.client.consumed.messages.error` | Counter | Messages permanently failed |
| `messaging.client.consumed.messages.retry` | Counter | Retry attempts |
| `messaging.process.duration` | Histogram (s) | Handler processing time |

All metrics are attributed with `messaging.message.type` and `messaging.destination.name`.

Dispatch metrics are recorded automatically by `DispatchMiddleware`.

### Custom Meter

```go
meter := otel.Meter("my-app")
queueotel.MetricsMiddlewareWithMeter(meter)
```
