// Package queue is a type-safe message queue library for Go with generics.
//
// Messages are plain Go structs that are JSON-encoded and dispatched through
// a transport (e.g. RabbitMQ). Handlers are registered per message type and
// receive the decoded struct directly — no manual JSON handling needed.
//
// The core API has three concepts:
//   - [Bus]: dispatches messages to transports and routes types to handlers
//   - [Worker]: consumes messages from transports and invokes handlers
//   - [Transport]: pluggable broker interface (AMQP, in-memory, custom)
package queue
