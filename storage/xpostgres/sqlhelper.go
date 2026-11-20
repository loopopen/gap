package xpostgres

import (
	"bytes"
	"fmt"
	"strings"
)

type WhereHelper struct {
	buff   bytes.Buffer
	params []any
}

// NewWhereHelper creates an empty SQL WHERE-clause builder.
func NewWhereHelper() *WhereHelper {
	return &WhereHelper{}
}

// Add appends a SQL condition and its parameter.
func Add[T comparable](h *WhereHelper, cond string, param T) {
	if h.buff.Len() == 0 {
		h.buff.WriteString("WHERE ")
	} else {
		h.buff.WriteString(" AND ")
	}
	h.params = append(h.params, param)
	cond = strings.Replace(cond, "?", fmt.Sprintf("$%d", len(h.params)), 1)
	h.buff.WriteString(cond)
}

// AddIfNotZero adds a condition and parameter when the value is non-zero.
func AddIfNotZero[T comparable](h *WhereHelper, cond string, v T) {
	var zero T
	if zero == v {
		return
	}
	Add(h, cond, v)
}

// String returns the string representation of the receiver.
func (h *WhereHelper) String() string {
	return h.buff.String()
}

// Params returns the accumulated SQL parameters.
func (h *WhereHelper) Params() []any {
	return h.params
}
