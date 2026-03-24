// Package postgres provides a PostgreSQL transport for go-queue using pgx.
//
// Messages are stored in a table and consumed using SELECT ... FOR UPDATE SKIP LOCKED.
// LISTEN/NOTIFY wakes consumers on new inserts to avoid polling.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shyim/go-queue"
)

// Config holds the configuration for the PostgreSQL transport.
type Config struct {
	// DSN is the PostgreSQL connection string (e.g. "postgres://user:pass@localhost:5432/db").
	DSN string

	// Table is the name of the queue table. Defaults to "queue_messages".
	Table string

	// Channel is the LISTEN/NOTIFY channel name. Defaults to "queue_notify".
	Channel string

	// PollInterval is the fallback polling interval when no NOTIFY is received.
	// Defaults to 5s.
	PollInterval time.Duration

	// ReaperInterval is how often stale processing messages are reclaimed.
	// Defaults to 1m.
	ReaperInterval time.Duration

	// VisibilityTimeout is how long a message can be in "processing" before
	// the reaper reclaims it. Defaults to 5m.
	VisibilityTimeout time.Duration
}

func applyDefaults(c Config) Config {
	if c.Table == "" {
		c.Table = "queue_messages"
	}
	if c.Channel == "" {
		c.Channel = "queue_notify"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.ReaperInterval <= 0 {
		c.ReaperInterval = 1 * time.Minute
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 5 * time.Minute
	}
	return c
}

// Transport implements queue.Transport using PostgreSQL.
type Transport struct {
	config Config
	pool   *pgxpool.Pool
}

// NewTransport creates a new PostgreSQL transport.
// It connects lazily on first use using Config.DSN.
func NewTransport(config Config) *Transport {
	return &Transport{
		config: applyDefaults(config),
	}
}

// NewTransportFromPool creates a new PostgreSQL transport using an existing pgxpool.Pool.
// The caller is responsible for closing the pool.
func NewTransportFromPool(pool *pgxpool.Pool, config Config) *Transport {
	return &Transport{
		config: applyDefaults(config),
		pool:   pool,
	}
}

func (t *Transport) connect(ctx context.Context) error {
	if t.pool != nil {
		return nil
	}
	pool, err := pgxpool.New(ctx, t.config.DSN)
	if err != nil {
		return fmt.Errorf("postgres: connect: %w", err)
	}
	t.pool = pool
	return nil
}

// Setup creates the queue table, index, and notify trigger.
func (t *Transport) Setup(ctx context.Context) error {
	if err := t.connect(ctx); err != nil {
		return err
	}

	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id            BIGSERIAL PRIMARY KEY,
			envelope_id   TEXT        NOT NULL,
			type          TEXT        NOT NULL,
			body          BYTEA       NOT NULL,
			headers       JSONB       NOT NULL DEFAULT '{}',
			status        TEXT        NOT NULL DEFAULT 'pending',
			attempts      INT         NOT NULL DEFAULT 0,
			max_attempts  INT         NOT NULL DEFAULT 0,
			last_error    TEXT,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at    TIMESTAMPTZ
		);

		CREATE INDEX IF NOT EXISTS idx_%s_dequeue
			ON %s (available_at, created_at)
			WHERE status = 'pending';

		CREATE OR REPLACE FUNCTION %s_notify() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('%s', NEW.id::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		DROP TRIGGER IF EXISTS trg_%s_notify ON %s;
		CREATE TRIGGER trg_%s_notify
			AFTER INSERT ON %s
			FOR EACH ROW EXECUTE FUNCTION %s_notify();
	`,
		t.config.Table,
		t.config.Table, t.config.Table,
		t.config.Table, t.config.Channel,
		t.config.Table, t.config.Table,
		t.config.Table, t.config.Table,
		t.config.Table,
	)

	_, err := t.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("postgres: setup: %w", err)
	}
	return nil
}

// Send inserts a message into the queue table.
func (t *Transport) Send(ctx context.Context, envelope *queue.Envelope) error {
	if err := t.connect(ctx); err != nil {
		return err
	}

	headers, err := json.Marshal(envelope.Headers)
	if err != nil {
		return fmt.Errorf("postgres: marshal headers: %w", err)
	}

	var availableAt time.Time
	if ds, ok := queue.GetStamp[queue.DelayStamp](envelope); ok && ds.Delay > 0 {
		availableAt = time.Now().Add(ds.Delay)
	} else {
		availableAt = time.Now()
	}

	var maxAttempts int
	if rs, ok := queue.GetStamp[queue.RedeliveryStamp](envelope); ok {
		maxAttempts = rs.MaxRetries
	}

	sql := fmt.Sprintf(`
		INSERT INTO %s (envelope_id, type, body, headers, max_attempts, available_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, t.config.Table)

	_, err = t.pool.Exec(ctx, sql,
		envelope.ID, envelope.Type, envelope.Body, headers, maxAttempts, availableAt,
	)
	if err != nil {
		return fmt.Errorf("postgres: send: %w", err)
	}
	return nil
}

// Receive returns a channel of envelopes. It uses LISTEN/NOTIFY to wake up
// on new inserts and falls back to polling at PollInterval.
func (t *Transport) Receive(ctx context.Context) (<-chan *queue.Envelope, error) {
	if err := t.connect(ctx); err != nil {
		return nil, err
	}

	out := make(chan *queue.Envelope)
	go t.consumeLoop(ctx, out)
	go t.reaperLoop(ctx)
	return out, nil
}

func (t *Transport) consumeLoop(ctx context.Context, out chan<- *queue.Envelope) {
	defer close(out)

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		slog.Error("postgres: acquire listen connection", "error", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", t.config.Channel))
	if err != nil {
		slog.Error("postgres: listen", "error", err)
		return
	}

	for {
		for {
			envelope, err := t.claim(ctx)
			if err != nil {
				slog.Error("postgres: claim", "error", err)
				break
			}
			if envelope == nil {
				break
			}
			select {
			case out <- envelope:
			case <-ctx.Done():
				return
			}
		}

		waitCtx, cancel := context.WithTimeout(ctx, t.config.PollInterval)
		_, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}
		if err != nil && waitCtx.Err() == nil {
			slog.Error("postgres: wait for notification", "error", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func (t *Transport) claim(ctx context.Context) (*queue.Envelope, error) {
	sql := fmt.Sprintf(`
		WITH next AS (
			SELECT id FROM %s
			WHERE status = 'pending' AND available_at <= NOW()
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s SET status = 'processing', started_at = NOW(), attempts = attempts + 1
		FROM next WHERE %s.id = next.id
		RETURNING %s.id, %s.envelope_id, %s.type, %s.body, %s.headers,
		          %s.attempts, %s.max_attempts, %s.last_error
	`, t.config.Table,
		t.config.Table, t.config.Table,
		t.config.Table, t.config.Table, t.config.Table, t.config.Table, t.config.Table,
		t.config.Table, t.config.Table, t.config.Table,
	)

	var (
		dbID        int64
		envelopeID  string
		msgType     string
		body        []byte
		headersJSON []byte
		attempts    int
		maxAttempts int
		lastError   *string
	)

	err := t.pool.QueryRow(ctx, sql).Scan(
		&dbID, &envelopeID, &msgType, &body, &headersJSON,
		&attempts, &maxAttempts, &lastError,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	headers := make(map[string]string)
	_ = json.Unmarshal(headersJSON, &headers)

	envelope := &queue.Envelope{
		Body:          body,
		Type:          msgType,
		ID:            envelopeID,
		Headers:       headers,
		TransportData: dbID,
	}

	if attempts > 1 || maxAttempts > 0 {
		le := ""
		if lastError != nil {
			le = *lastError
		}
		envelope.AddStamp(queue.RedeliveryStamp{
			Attempt:    attempts - 1,
			MaxRetries: maxAttempts,
			LastError:  le,
		})
	}

	return envelope, nil
}

// Ack marks a message as done.
func (t *Transport) Ack(ctx context.Context, envelope *queue.Envelope) error {
	dbID, ok := envelope.TransportData.(int64)
	if !ok {
		return fmt.Errorf("postgres: ack: missing db id")
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE id = $1", t.config.Table)
	_, err := t.pool.Exec(ctx, sql, dbID)
	if err != nil {
		return fmt.Errorf("postgres: ack: %w", err)
	}
	return nil
}

// Nack marks a message as failed. If requeue is false, the message is deleted.
func (t *Transport) Nack(ctx context.Context, envelope *queue.Envelope, requeue bool) error {
	dbID, ok := envelope.TransportData.(int64)
	if !ok {
		return fmt.Errorf("postgres: nack: missing db id")
	}

	if !requeue {
		sql := fmt.Sprintf("DELETE FROM %s WHERE id = $1", t.config.Table)
		_, err := t.pool.Exec(ctx, sql, dbID)
		if err != nil {
			return fmt.Errorf("postgres: nack delete: %w", err)
		}
		return nil
	}

	sql := fmt.Sprintf(`
		UPDATE %s SET status = 'pending', available_at = NOW(), started_at = NULL
		WHERE id = $1
	`, t.config.Table)
	_, err := t.pool.Exec(ctx, sql, dbID)
	if err != nil {
		return fmt.Errorf("postgres: nack requeue: %w", err)
	}
	return nil
}

// Retry updates the message with new retry metadata and puts it back as pending.
func (t *Transport) Retry(ctx context.Context, envelope *queue.Envelope) error {
	dbID, ok := envelope.TransportData.(int64)
	if !ok {
		return fmt.Errorf("postgres: retry: missing db id")
	}

	redelivery, _ := queue.GetLastStamp[queue.RedeliveryStamp](envelope)

	sql := fmt.Sprintf(`
		UPDATE %s
		SET status = 'pending',
		    available_at = NOW(),
		    started_at = NULL,
		    max_attempts = $2,
		    last_error = $3
		WHERE id = $1
	`, t.config.Table)

	_, err := t.pool.Exec(ctx, sql, dbID, redelivery.MaxRetries, redelivery.LastError)
	if err != nil {
		return fmt.Errorf("postgres: retry: %w", err)
	}
	return nil
}

// IsConnected reports whether the pool has active connections.
func (t *Transport) IsConnected() bool {
	if t.pool == nil {
		return false
	}
	return t.pool.Stat().TotalConns() > 0
}

// Close closes the connection pool.
func (t *Transport) Close() error {
	if t.pool != nil {
		t.pool.Close()
	}
	return nil
}

func (t *Transport) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(t.config.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.reclaim(ctx)
		}
	}
}

func (t *Transport) reclaim(ctx context.Context) {
	sql := fmt.Sprintf(`
		UPDATE %s
		SET status = 'pending', available_at = NOW(), started_at = NULL
		WHERE status = 'processing'
		  AND started_at < NOW() - $1::interval
	`, t.config.Table)

	tag, err := t.pool.Exec(ctx, sql, t.config.VisibilityTimeout.String())
	if err != nil {
		slog.Error("postgres: reclaim", "error", err)
		return
	}
	if tag.RowsAffected() > 0 {
		slog.Warn("postgres: reclaimed stale messages", "count", tag.RowsAffected())
	}
}
