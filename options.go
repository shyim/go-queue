package queue

import "time"

// Option configures how a message is dispatched.
type Option func(*dispatchOptions)

type dispatchOptions struct {
	delay      time.Duration
	maxRetries *int
	queue      string
	headers    map[string]string
}

func newDispatchOptions(opts ...Option) dispatchOptions {
	o := dispatchOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithDelay defers processing by the given duration.
func WithDelay(d time.Duration) Option {
	return func(o *dispatchOptions) {
		o.delay = d
	}
}

// WithMaxRetries overrides the default max retry count for this message.
func WithMaxRetries(n int) Option {
	return func(o *dispatchOptions) {
		o.maxRetries = &n
	}
}

// WithQueue routes the message to a specific named transport.
func WithQueue(name string) Option {
	return func(o *dispatchOptions) {
		o.queue = name
	}
}

// WithHeader adds a custom header to the envelope.
func WithHeader(key, value string) Option {
	return func(o *dispatchOptions) {
		if o.headers == nil {
			o.headers = make(map[string]string)
		}
		o.headers[key] = value
	}
}

// RetryStrategy defines how failed messages are retried.
type RetryStrategy struct {
	MaxRetries int
	Delay      time.Duration
	Multiplier float64 // 1.0 = fixed, 2.0 = exponential backoff
	MaxDelay   time.Duration
	Jitter     float64 // 0.0 to 1.0
}

// DefaultRetryStrategy returns a default retry strategy.
func DefaultRetryStrategy() RetryStrategy {
	return RetryStrategy{
		MaxRetries: 3,
		Delay:      1 * time.Second,
		Multiplier: 2.0,
		MaxDelay:   30 * time.Second,
		Jitter:     0.1,
	}
}
