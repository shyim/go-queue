package queue

import "errors"

// UnrecoverableError signals that the message should not be retried.
type UnrecoverableError struct {
	Err error
}

func (e *UnrecoverableError) Error() string { return e.Err.Error() }
func (e *UnrecoverableError) Unwrap() error { return e.Err }

// Unrecoverable marks an error as unrecoverable, skipping all retries.
func Unrecoverable(err error) error {
	return &UnrecoverableError{Err: err}
}

// IsUnrecoverable reports whether the error is unrecoverable.
func IsUnrecoverable(err error) bool {
	var ue *UnrecoverableError
	return errors.As(err, &ue)
}
