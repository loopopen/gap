package event

// import (
// 	"encoding/json"
// 	"reflect"
// )

// type Event[T any] struct {
// 	Data T
// }

// func NewEvent[T any](data T) *Event[T] {
// 	return &Event[T]{
// 		Data: data,
// 	}
// }

// func (e *Event[T]) Topic() string {
// 	t := reflect.TypeOf((*T)(nil)).Elem()
// 	if t.Kind() == reflect.Ptr {
// 		t = t.Elem()
// 	}
// 	return t.String()
// }

// func (e *Event[T]) UnmarshalJSON(data []byte) error {
// 	var t T
// 	err := json.Unmarshal(data, &t)
// 	if err != nil {
// 		return err
// 	}
// 	e.Data = t
// 	return nil
// }

// func (e *Event[T]) MarshalJSON() ([]byte, error) {
// 	return json.Marshal(e.Data)
// }
