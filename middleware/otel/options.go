package otel

import (
	"strings"

	"github.com/shyim/go-queue"
)

type SpanNameNormalizer func(messageType string) string

type spanNameFormatter func(envelope *queue.Envelope, operation string) string

type Option func(*config)

type config struct {
	spanNameFormatter spanNameFormatter
}

func newConfig(opts ...Option) config {
	cfg := config{
		spanNameFormatter: defaultSpanNameFormatter,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// WithSpanNameNormalizer normalizes the message type before it is used in the
// span name. Span attributes still use the original envelope type.
func WithSpanNameNormalizer(normalizer SpanNameNormalizer) Option {
	return func(cfg *config) {
		if normalizer == nil {
			cfg.spanNameFormatter = defaultSpanNameFormatter
			return
		}

		cfg.spanNameFormatter = func(envelope *queue.Envelope, operation string) string {
			messageType := envelope.Type
			if messageType != "" {
				messageType = normalizer(messageType)
			}

			return formatSpanName(messageType, operation)
		}
	}
}

// DefaultSpanNameNormalizer trims a fully-qualified Go type name down to the
// struct name that is usually most useful in span names.
func DefaultSpanNameNormalizer(messageType string) string {
	if lastSlash := strings.LastIndex(messageType, "/"); lastSlash >= 0 {
		messageType = messageType[lastSlash+1:]
	}

	if lastDot := strings.LastIndex(messageType, "."); lastDot >= 0 {
		messageType = messageType[lastDot+1:]
	}

	return messageType
}

func defaultSpanNameFormatter(envelope *queue.Envelope, operation string) string {
	return formatSpanName(envelope.Type, operation)
}

func formatSpanName(messageType, operation string) string {
	if messageType == "" {
		return operation
	}

	return messageType + " " + operation
}
