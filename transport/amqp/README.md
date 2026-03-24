# AMQP Transport

RabbitMQ / LavinMQ transport for go-queue using AMQP 0.9.1.

```sh
go get github.com/shyim/go-queue
```

## Usage

```go
import "github.com/shyim/go-queue/transport/amqp"

transport := amqp.NewTransport(amqp.Config{
	DSN:      "amqp://guest:guest@localhost:5672/",
	Exchange: "myapp",
	Queue:    "tasks",
})
defer transport.Close()

bus := queue.NewBus()
bus.AddTransport("async", transport)
bus.Setup(ctx)
```

## Configuration

```go
amqp.Config{
	DSN:               "amqp://guest:guest@localhost:5672/",
	Exchange:          "myapp",           // exchange name (default: "messages")
	ExchangeType:      "direct",          // direct, topic, fanout, headers (default: "direct")
	Queue:             "tasks",           // queue name (default: "default")
	RoutingKey:        "tasks",           // routing key (default: "default")
	PrefetchCount:     10,                // consumer prefetch (default: 10)
	Durable:           true,              // survive broker restart (default: true)
	AutoSetup:         true,              // auto-declare exchange/queue/binding (default: true)
	ReconnectDelay:    5 * time.Second,   // delay between reconnection attempts (default: 5s)
	PublisherConfirms: true,              // wait for broker ack on publish (default: true)
	DelayedExchange:   false,             // see Delayed Messages below
	Deduplication:     nil,               // see Deduplication below
}
```

## Features

### Auto-Reconnection

The transport automatically reconnects on connection or channel failure. Separate channels are used for publishing and consuming.

### Delayed Messages

Requires LavinMQ (native) or the [rabbitmq_delayed_message_exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) plugin.

Set `DelayedExchange: true` to declare the exchange as `x-delayed-message` type. The configured `ExchangeType` becomes the `x-delayed-type` argument.

```go
transport := amqp.NewTransport(amqp.Config{
	DSN:             "amqp://localhost:5672/",
	Exchange:        "delayed",
	ExchangeType:    "direct",
	DelayedExchange: true,
})

queue.Dispatch(ctx, bus, msg, queue.WithDelay(10*time.Second))
```

### Message Deduplication

Requires LavinMQ (native) or the [rabbitmq-message-deduplication](https://github.com/noxdafox/rabbitmq-message-deduplication) plugin.

```go
transport := amqp.NewTransport(amqp.Config{
	DSN: "amqp://localhost:5672/",
	Deduplication: &amqp.DeduplicationConfig{
		CacheSize: 10000,       // number of IDs to remember
		CacheTTL:  60000,       // milliseconds, 0 = unlimited
	},
})
```

The queue is declared with `x-message-deduplication: true`. Each published message gets an `x-deduplication-header` set to the envelope ID. Retries use `{id}-retry-{attempt}` to avoid being dropped as duplicates.

### Health Check

```go
if transport.IsConnected() {
	// ready
}
```

### Publisher Confirms

When `PublisherConfirms: true` (default), `Send` blocks until the broker confirms receipt. If the broker nacks, `Send` returns an error.

## Error Handling

- `amqp.ErrTransportClosed` — returned when calling `Send`, `Ack`, `Nack`, or `Retry` after `Close()`
