package xmysql

import "bytes"

type WhereHelper struct {
	buff   bytes.Buffer
	params []any
}

func NewWhereHelper() *WhereHelper {
	return &WhereHelper{}
}

func Add[T comparable](h *WhereHelper, cond string, param T) {
	if h.buff.Len() == 0 {
		h.buff.WriteString("WHERE ")
	} else {
		h.buff.WriteString(" AND ")
	}
	h.buff.WriteString(cond)
	h.params = append(h.params, param)
}

func AddIfNotZero[T comparable](h *WhereHelper, cond string, v T) {
	var zero T
	if zero == v {
		return
	}
	Add(h, cond, v)
}

func (h *WhereHelper) String() string {
	return h.buff.String()
}

func (h *WhereHelper) Params() []any {
	return h.params
}
