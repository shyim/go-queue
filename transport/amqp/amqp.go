// Package amqp provides a RabbitMQ/LavinMQ transport for go-queue using AMQP 0.9.1.
package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/shyim/go-queue"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// ErrTransportClosed is returned when operations are attempted on a closed transport.
var ErrTransportClosed = errors.New("amqp: transport is closed")

const (
	headerRetryAttempt = "x-retry-attempt"
	headerRetryMax     = "x-retry-max"
	headerRetryError   = "x-retry-error"
)

// Config holds the configuration for the AMQP transport.
type Config struct {
	DSN               string
	Exchange          string
	ExchangeType      string
	Queue             string
	RoutingKey        string
	PrefetchCount     int
	Durable           bool
	AutoSetup         bool
	ReconnectDelay    time.Duration
	PublisherConfirms bool
	// DelayedExchange declares the exchange as x-delayed-message type (LavinMQ native,
	// or rabbitmq_delayed_message_exchange plugin). ExchangeType becomes the x-delayed-type
	// argument (e.g. "direct", "topic"). Messages with a DelayStamp get the x-delay header.
	DelayedExchange bool
	// Deduplication enables LavinMQ/RabbitMQ message deduplication on the queue.
	// When non-nil, the queue is declared with x-message-deduplication and each
	// published message gets an x-deduplication-header set to the envelope ID.
	Deduplication *DeduplicationConfig
}

// DeduplicationConfig enables broker-level message deduplication.
type DeduplicationConfig struct {
	// CacheSize is the number of dedup IDs to keep in memory. Required.
	CacheSize int
	// CacheTTL is how long a dedup ID is remembered (milliseconds). 0 = unlimited.
	CacheTTL int
}

func applyDefaults(c Config) Config {
	if c.Exchange == "" {
		c.Exchange = "messages"
	}
	if c.ExchangeType == "" {
		c.ExchangeType = "direct"
	}
	if c.Queue == "" {
		c.Queue = "default"
	}
	if c.RoutingKey == "" {
		c.RoutingKey = "default"
	}
	if c.PrefetchCount == 0 {
		c.PrefetchCount = 10
	}
	if c.ReconnectDelay == 0 {
		c.ReconnectDelay = 5 * time.Second
	}
	return c
}

// Transport implements queue.Transport using RabbitMQ.
type Transport struct {
	config Config

	mu        sync.Mutex
	conn      *amqp091.Connection
	pubChan   *amqp091.Channel
	conChan   *amqp091.Channel
	closed    bool
	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewTransport creates a new AMQP transport with defaults applied.
func NewTransport(config Config) *Transport {
	return &Transport{
		config:  applyDefaults(config),
		closeCh: make(chan struct{}),
	}
}

func (t *Transport) connect() (*amqp091.Connection, error) {
	conn, err := amqp091.Dial(t.config.DSN)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	return conn, nil
}

func (t *Transport) ensureConnection() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTransportClosed
	}

	if t.conn != nil && !t.conn.IsClosed() {
		return nil
	}

	conn, err := t.connect()
	if err != nil {
		return err
	}
	t.conn = conn
	t.pubChan = nil
	t.conChan = nil
	return nil
}

func (t *Transport) ensurePublishChannel() (*amqp091.Channel, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, ErrTransportClosed
	}

	if t.pubChan != nil {
		return t.pubChan, nil
	}

	if t.conn == nil || t.conn.IsClosed() {
		conn, err := t.connect()
		if err != nil {
			return nil, err
		}
		t.conn = conn
		t.conChan = nil
	}

	ch, err := t.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp open publish channel: %w", err)
	}

	if t.config.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("amqp enable confirms: %w", err)
		}
	}

	t.pubChan = ch
	return ch, nil
}

func (t *Transport) Setup(ctx context.Context) error {
	if err := t.ensureConnection(); err != nil {
		return fmt.Errorf("amqp setup connection: %w", err)
	}

	t.mu.Lock()
	conn := t.conn
	t.mu.Unlock()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp setup channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	durable := t.config.Durable

	if err := ch.ExchangeDeclare(
		t.config.Exchange,
		t.exchangeType(),
		durable,
		false, false, false,
		t.exchangeArgs(),
	); err != nil {
		return fmt.Errorf("amqp declare exchange: %w", err)
	}

	_, err = ch.QueueDeclare(
		t.config.Queue,
		durable,
		false, false, false,
		t.queueArgs(),
	)
	if err != nil {
		return fmt.Errorf("amqp declare queue: %w", err)
	}

	if err := ch.QueueBind(
		t.config.Queue,
		t.config.RoutingKey,
		t.config.Exchange,
		false, nil,
	); err != nil {
		return fmt.Errorf("amqp bind queue: %w", err)
	}

	slog.Info("amqp: setup complete",
		"exchange", t.config.Exchange,
		"queue", t.config.Queue,
		"routing_key", t.config.RoutingKey,
	)
	return nil
}

func (t *Transport) Send(ctx context.Context, envelope *queue.Envelope) error {
	ch, err := t.ensurePublishChannel()
	if err != nil {
		return err
	}

	headers := amqp091.Table{}
	if envelope.Type != "" {
		headers["type"] = envelope.Type
	}
	for k, v := range envelope.Headers {
		headers[k] = v
	}

	if ds, ok := queue.GetStamp[queue.DelayStamp](envelope); ok && ds.Delay > 0 {
		headers["x-delay"] = int64(ds.Delay / time.Millisecond)
	}
	if t.config.Deduplication != nil && envelope.ID != "" {
		headers["x-deduplication-header"] = envelope.ID
	}

	pub := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    envelope.ID,
		Headers:      headers,
		Body:         envelope.Body,
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		t.config.Exchange,
		t.config.RoutingKey,
		false, false,
		pub,
	)
	if err != nil {
		t.mu.Lock()
		t.pubChan = nil
		t.mu.Unlock()
		return fmt.Errorf("amqp publish: %w", err)
	}

	if t.config.PublisherConfirms {
		if !confirm.Wait() {
			return fmt.Errorf("amqp publish: server nacked message %s", envelope.ID)
		}
	}

	slog.Debug("amqp: sent message", "id", envelope.ID, "type", envelope.Type)
	return nil
}

func (t *Transport) Receive(ctx context.Context) (<-chan *queue.Envelope, error) {
	out := make(chan *queue.Envelope)

	go t.consumeLoop(ctx, out)

	return out, nil
}

func (t *Transport) consumeLoop(ctx context.Context, out chan<- *queue.Envelope) {
	defer close(out)

	for {
		select {
		case <-t.closeCh:
			return
		default:
		}
		if ctx.Err() != nil {
			return
		}

		if err := t.ensureConnection(); err != nil {
			slog.Error("amqp: connection failed, retrying", "error", err, "delay", t.config.ReconnectDelay)
			select {
			case <-ctx.Done():
				return
			case <-t.closeCh:
				return
			case <-time.After(t.config.ReconnectDelay):
				continue
			}
		}

		t.mu.Lock()
		conn := t.conn
		t.mu.Unlock()

		connClosed := conn.NotifyClose(make(chan *amqp091.Error, 1))

		ch, err := conn.Channel()
		if err != nil {
			slog.Error("amqp: open consume channel failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.config.ReconnectDelay):
				continue
			}
		}

		if err := ch.Qos(t.config.PrefetchCount, 0, false); err != nil {
			slog.Error("amqp: set qos failed", "error", err)
			_ = ch.Close()
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.config.ReconnectDelay):
				continue
			}
		}

		if t.config.AutoSetup {
			if err := t.setupOnChannel(ch); err != nil {
				slog.Error("amqp: auto-setup failed", "error", err)
				_ = ch.Close()
				select {
				case <-ctx.Done():
					return
				case <-time.After(t.config.ReconnectDelay):
					continue
				}
			}
		}

		chClosed := ch.NotifyClose(make(chan *amqp091.Error, 1))

		deliveries, err := ch.ConsumeWithContext(
			ctx,
			t.config.Queue,
			"",
			false, false, false, false,
			nil,
		)
		if err != nil {
			slog.Error("amqp: consume failed", "error", err)
			_ = ch.Close()
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.config.ReconnectDelay):
				continue
			}
		}

		t.mu.Lock()
		t.conChan = ch
		t.mu.Unlock()

		slog.Info("amqp: consumer started", "queue", t.config.Queue)

		if t.drainDeliveries(ctx, deliveries, out, connClosed, chClosed) {
			go func() { _ = ch.Close() }()
			return
		}

		go func() { _ = ch.Close() }()

		t.mu.Lock()
		t.conChan = nil
		t.mu.Unlock()

		slog.Warn("amqp: consumer disconnected, reconnecting", "delay", t.config.ReconnectDelay)
		select {
		case <-ctx.Done():
			return
		case <-t.closeCh:
			return
		case <-time.After(t.config.ReconnectDelay):
		}
	}
}

func (t *Transport) drainDeliveries(
	ctx context.Context,
	deliveries <-chan amqp091.Delivery,
	out chan<- *queue.Envelope,
	connClosed <-chan *amqp091.Error,
	chClosed <-chan *amqp091.Error,
) bool {
	for {
		select {
		case <-ctx.Done():
			return true
		case err := <-connClosed:
			slog.Warn("amqp: connection closed", "error", err)
			t.mu.Lock()
			t.conn = nil
			t.pubChan = nil
			t.mu.Unlock()
			return false
		case err := <-chClosed:
			slog.Warn("amqp: channel closed", "error", err)
			return false
		case d, ok := <-deliveries:
			if !ok {
				return false
			}
			env := deliveryToEnvelope(d)
			select {
			case out <- env:
			case <-ctx.Done():
				return true
			}
		}
	}
}

func (t *Transport) setupOnChannel(ch *amqp091.Channel) error {
	durable := t.config.Durable

	if err := ch.ExchangeDeclare(
		t.config.Exchange,
		t.exchangeType(),
		durable,
		false, false, false,
		t.exchangeArgs(),
	); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		t.config.Queue,
		durable,
		false, false, false,
		t.queueArgs(),
	); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.QueueBind(
		t.config.Queue,
		t.config.RoutingKey,
		t.config.Exchange,
		false, nil,
	); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	return nil
}

func (t *Transport) queueArgs() amqp091.Table {
	if t.config.Deduplication == nil {
		return nil
	}
	args := amqp091.Table{
		"x-message-deduplication": true,
		"x-cache-size":           int32(t.config.Deduplication.CacheSize),
	}
	if t.config.Deduplication.CacheTTL > 0 {
		args["x-cache-ttl"] = int32(t.config.Deduplication.CacheTTL)
	}
	return args
}

func (t *Transport) exchangeType() string {
	if t.config.DelayedExchange {
		return "x-delayed-message"
	}
	return t.config.ExchangeType
}

func (t *Transport) exchangeArgs() amqp091.Table {
	if !t.config.DelayedExchange {
		return nil
	}
	return amqp091.Table{
		"x-delayed-type": t.config.ExchangeType,
	}
}

func deliveryToEnvelope(d amqp091.Delivery) *queue.Envelope {
	env := &queue.Envelope{
		Body:        d.Body,
		ID:          d.MessageId,
		TransportData: d.DeliveryTag,
		Headers:     make(map[string]string),
	}

	var attempt, maxRetries int
	var lastError string

	for k, v := range d.Headers {
		switch k {
		case "type":
			if s, ok := v.(string); ok {
				env.Type = s
			}
		case headerRetryAttempt:
			attempt = amqpHeaderInt(v)
		case headerRetryMax:
			maxRetries = amqpHeaderInt(v)
		case headerRetryError:
			if s, ok := v.(string); ok {
				lastError = s
			}
		default:
			switch val := v.(type) {
			case string:
				env.Headers[k] = val
			default:
				env.Headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	if attempt > 0 || maxRetries > 0 {
		env.AddStamp(queue.RedeliveryStamp{
			Attempt:    attempt,
			MaxRetries: maxRetries,
			LastError:  lastError,
		})
	}

	return env
}

func amqpHeaderInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	default:
		return 0
	}
}

func (t *Transport) Ack(ctx context.Context, envelope *queue.Envelope) error {
	t.mu.Lock()
	ch := t.conChan
	t.mu.Unlock()

	if ch == nil {
		return fmt.Errorf("amqp ack: no consume channel")
	}

	tag, ok := envelope.TransportData.(uint64)
	if !ok {
		return fmt.Errorf("amqp ack: missing delivery tag")
	}

	if err := ch.Ack(tag, false); err != nil {
		return fmt.Errorf("amqp ack: %w", err)
	}
	return nil
}

func (t *Transport) Retry(ctx context.Context, envelope *queue.Envelope) error {
	redelivery, _ := queue.GetLastStamp[queue.RedeliveryStamp](envelope)

	headers := amqp091.Table{}
	if envelope.Type != "" {
		headers["type"] = envelope.Type
	}
	headers[headerRetryAttempt] = int32(redelivery.Attempt)
	headers[headerRetryMax] = int32(redelivery.MaxRetries)
	if redelivery.LastError != "" {
		headers[headerRetryError] = redelivery.LastError
	}
	if t.config.Deduplication != nil && envelope.ID != "" {
		headers["x-deduplication-header"] = fmt.Sprintf("%s-retry-%d", envelope.ID, redelivery.Attempt)
	}
	for k, v := range envelope.Headers {
		headers[k] = v
	}

	ch, err := t.ensurePublishChannel()
	if err != nil {
		return fmt.Errorf("amqp retry publish channel: %w", err)
	}

	pub := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		MessageId:    envelope.ID,
		Headers:      headers,
		Body:         envelope.Body,
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		t.config.Exchange,
		t.config.RoutingKey,
		false, false,
		pub,
	)
	if err != nil {
		t.mu.Lock()
		t.pubChan = nil
		t.mu.Unlock()
		return fmt.Errorf("amqp retry publish: %w", err)
	}

	if t.config.PublisherConfirms {
		if !confirm.Wait() {
			return fmt.Errorf("amqp retry: server nacked message %s", envelope.ID)
		}
	}

	if err := t.Ack(ctx, envelope); err != nil {
		slog.Warn("amqp retry: ack failed after publish", "error", err, "id", envelope.ID)
	}

	return nil
}

func (t *Transport) Nack(ctx context.Context, envelope *queue.Envelope, requeue bool) error {
	t.mu.Lock()
	ch := t.conChan
	t.mu.Unlock()

	if ch == nil {
		return fmt.Errorf("amqp nack: no consume channel")
	}

	tag, ok := envelope.TransportData.(uint64)
	if !ok {
		return fmt.Errorf("amqp nack: missing delivery tag")
	}

	if err := ch.Nack(tag, false, requeue); err != nil {
		return fmt.Errorf("amqp nack: %w", err)
	}
	return nil
}

// IsConnected reports whether the transport has an active connection.
func (t *Transport) IsConnected() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn != nil && !t.conn.IsClosed()
}

func (t *Transport) Close() error {
	t.closeOnce.Do(func() { close(t.closeCh) })

	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	var firstErr error

	if t.pubChan != nil {
		if err := t.pubChan.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		t.pubChan = nil
	}

	if t.conChan != nil {
		if err := t.conChan.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		t.conChan = nil
	}

	if t.conn != nil && !t.conn.IsClosed() {
		if err := t.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		t.conn = nil
	}

	return firstErr
}
