package queue

import "context"

type contextKey struct{}

var envelopeKey = contextKey{}

// WithEnvelope returns a new context with the envelope attached.
func WithEnvelope(ctx context.Context, env *Envelope) context.Context {
	return context.WithValue(ctx, envelopeKey, env)
}

// EnvelopeFromContext returns the envelope from the context, or nil.
func EnvelopeFromContext(ctx context.Context) *Envelope {
	env, _ := ctx.Value(envelopeKey).(*Envelope)
	return env
}
