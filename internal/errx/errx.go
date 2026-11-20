package errx

import (
	"errors"
	"fmt"
)

// Permanent marks an error that another attempt cannot repair.
// HandleAndUpdate records those messages as Failed instead of scheduling a retry.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether an error is marked permanent.
func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}

type permanentError struct {
	error
}

// Unwrap returns the wrapped error.
func (e *permanentError) Unwrap() error { return e.error }

// ErrParamIsNil creates an error for a required nil parameter.
func ErrParamIsNil(param string) error {
	return fmt.Errorf("param %s is nil", param)
}

// ErrMultiHandlers creates an error for duplicate handlers of one topic.
func ErrMultiHandlers(topic, group string) error {
	return fmt.Errorf("topic %s(group: %s) has mutiple handers", topic, group)
}

// ErrHandlerNotFound creates an error for a missing topic handler.
func ErrHandlerNotFound(topic, group string) error {
	return fmt.Errorf("no handler found for topic %s(group: %s)", topic, group)
}

var (
	ErrNoStorage      = errors.New("no storage instance configured")
	ErrNoBroker       = errors.New("no broker instance configured")
	ErrNilPayload     = errors.New("payload is nil")
	ErrEmptyTopic     = errors.New("topic is empty")
	ErrInvalidGormTx  = errors.New("tx is not a gorm transaction")
	ErrInvalidSQLTx   = errors.New("tx is not a sql transaction")
	ErrNilHandler     = errors.New("handler is nil")
	ErrTxMultiBinding = errors.New("multiple bindings to transaction")
)
