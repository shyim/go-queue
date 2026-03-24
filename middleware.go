package queue

import "context"

// Middleware wraps handler invocation. Call next to continue the chain.
type Middleware func(ctx context.Context, envelope *Envelope, next func(ctx context.Context, envelope *Envelope) error) error
