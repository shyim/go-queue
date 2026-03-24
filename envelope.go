package queue

import "time"

// Envelope wraps a serialized message with metadata stamps.
type Envelope struct {
	Body      []byte
	Type      string
	ID        string
	Stamps    []Stamp
	Headers   map[string]string
	Transport string
	// TransportData holds transport-specific state (e.g. AMQP delivery tag).
	// The core library never reads this; it is owned by the transport implementation.
	TransportData any
}

// Stamp represents a piece of metadata attached to an envelope.
type Stamp interface {
	StampName() string
}

// GetStamp returns the first stamp of type S, or its zero value.
func GetStamp[S Stamp](e *Envelope) (S, bool) {
	for _, s := range e.Stamps {
		if typed, ok := s.(S); ok {
			return typed, true
		}
	}
	var zero S
	return zero, false
}

// GetLastStamp returns the last stamp of type S, or its zero value.
func GetLastStamp[S Stamp](e *Envelope) (S, bool) {
	var zero S
	found := false
	var result S
	for _, s := range e.Stamps {
		if typed, ok := s.(S); ok {
			result = typed
			found = true
		}
	}
	if found {
		return result, true
	}
	return zero, false
}

func (e *Envelope) AddStamp(s Stamp) {
	e.Stamps = append(e.Stamps, s)
}

// DelayStamp defers processing of the message.
type DelayStamp struct {
	Delay time.Duration
}

func (s DelayStamp) StampName() string { return "delay" }

// RedeliveryStamp tracks retry attempts.
type RedeliveryStamp struct {
	Attempt    int
	MaxRetries int
	LastError  string
}

func (s RedeliveryStamp) StampName() string { return "redelivery" }

// SentStamp records that a message was sent to a transport.
type SentStamp struct {
	TransportName string
	SentAt        time.Time
}

func (s SentStamp) StampName() string { return "sent" }

// ReceivedStamp marks that a message was received from a transport.
type ReceivedStamp struct {
	TransportName string
	ReceivedAt    time.Time
}

func (s ReceivedStamp) StampName() string { return "received" }

// HandledStamp records that a handler has processed the message.
type HandledStamp struct {
	HandlerName string
	HandledAt   time.Time
}

func (s HandledStamp) StampName() string { return "handled" }
