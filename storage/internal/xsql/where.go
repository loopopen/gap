package xsql

import (
	"bytes"
	"fmt"
	"strings"
)

// WhereBuilder accumulates a WHERE clause and its positional parameters.
type WhereBuilder struct {
	dialect Dialect
	buff    bytes.Buffer
	params  []any
}

func NewWhereBuilder(dialect Dialect) *WhereBuilder {
	return &WhereBuilder{dialect: dialect}
}

func Add[T comparable](h *WhereBuilder, condition string, value T) {
	if h.buff.Len() == 0 {
		h.buff.WriteString("WHERE ")
	} else {
		h.buff.WriteString(" AND ")
	}
	h.params = append(h.params, value)
	if h.dialect == Postgres {
		condition = strings.Replace(condition, "?", fmt.Sprintf("$%d", len(h.params)), 1)
	}
	h.buff.WriteString(condition)
}

func AddIfNotZero[T comparable](h *WhereBuilder, condition string, value T) {
	var zero T
	if zero != value {
		Add(h, condition, value)
	}
}

func (h *WhereBuilder) String() string {
	return h.buff.String()
}

func (h *WhereBuilder) Params() []any {
	return h.params
}
