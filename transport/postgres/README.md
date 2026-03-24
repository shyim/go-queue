# PostgreSQL Transport

PostgreSQL transport for go-queue using [pgx](https://github.com/jackc/pgx). No external broker needed.

Messages are stored in a table and consumed using `SELECT ... FOR UPDATE SKIP LOCKED`. `LISTEN/NOTIFY` wakes consumers on new inserts.

```sh
go get github.com/shyim/go-queue
```

## Usage

```go
import "github.com/shyim/go-queue/transport/postgres"

transport := postgres.NewTransport(postgres.Config{
	DSN:   "postgres://user:pass@localhost:5432/mydb",
	Table: "queue_messages",
})
defer transport.Close()

bus := queue.NewBus()
bus.AddTransport("pg", transport)
bus.Setup(ctx) // creates table, index, notify trigger
```

### Using an Existing Pool

```go
import "github.com/jackc/pgx/v5/pgxpool"

pool, _ := pgxpool.New(ctx, "postgres://...")
transport := postgres.NewTransportFromPool(pool, postgres.Config{
	Table: "queue_messages",
})
// Caller is responsible for closing the pool.
```

## Configuration

```go
postgres.Config{
	DSN:               "postgres://...",
	Table:             "queue_messages",   // table name (default: "queue_messages")
	Channel:           "queue_notify",     // LISTEN/NOTIFY channel (default: "queue_notify")
	PollInterval:      5 * time.Second,    // fallback poll interval (default: 5s)
	ReaperInterval:    1 * time.Minute,    // stale message reclaim interval (default: 1m)
	VisibilityTimeout: 5 * time.Minute,    // max processing time before reclaim (default: 5m)
}
```

## How It Works

### Table Schema

`Setup()` creates the following table (if it doesn't exist):

```sql
CREATE TABLE IF NOT EXISTS queue_messages (
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
```

A partial index on `(available_at, created_at) WHERE status = 'pending'` is created for efficient dequeue queries. A notify trigger fires on each insert.

### Consuming

1. Consumer runs `LISTEN queue_notify` on a dedicated connection
2. On notification (or poll timeout), claims a message atomically:
   ```sql
   WITH next AS (
       SELECT id FROM queue_messages
       WHERE status = 'pending' AND available_at <= NOW()
       ORDER BY created_at LIMIT 1
       FOR UPDATE SKIP LOCKED
   )
   UPDATE queue_messages SET status = 'processing', started_at = NOW(), attempts = attempts + 1
   FROM next WHERE queue_messages.id = next.id
   RETURNING ...
   ```
3. Multiple workers can consume concurrently — each gets a different message

### Acknowledgment

- **Ack** — deletes the row
- **Nack (requeue=true)** — sets status back to `pending`
- **Nack (requeue=false)** — deletes the row
- **Retry** — sets status back to `pending` with updated `max_attempts` and `last_error`

### Delayed Messages

`WithDelay` sets `available_at` in the future. The message won't be claimed until that time:

```go
queue.Dispatch(ctx, bus, msg, queue.WithDelay(10*time.Minute))
```

### Visibility Timeout

A background reaper goroutine runs at `ReaperInterval` and reclaims messages stuck in `processing` longer than `VisibilityTimeout`:

```sql
UPDATE queue_messages SET status = 'pending', available_at = NOW()
WHERE status = 'processing' AND started_at < NOW() - interval '5 minutes'
```

### Health Check

```go
if transport.IsConnected() {
	// pool has active connections
}
```
