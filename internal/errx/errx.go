package errx

import (
	"errors"
	"fmt"
)

func ErrParamIsNil(param string) error {
	return fmt.Errorf("param %s is nil", param)
}

func ErrMultiHandlers(topic, group string) error {
	return fmt.Errorf("topic %s(group: %s) has mutiple handers", topic, group)
}

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
