package gap

import (
	"context"
	"encoding/json"

	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/shoot"
)

type Handler[T any] func(ctx context.Context, msg T, headers map[string]string) error

func RegisterHandlerOnly(o *Options) bool {
	return o._registerHandlerOnly
}

func checkType[T any]() {
	var t T
	err := json.Unmarshal([]byte("null"), &t)
	if err != nil {
		panic(err)
	}
}

func HandleTopicWithinGroup[T any](handler Handler[T], topic string, group string) shoot.Option[Options, *Options] {
	checkType[T]()
	if handler == nil {
		panic(errx.ErrParamIsNil("handler"))
	}
	h := func(ctx context.Context, msg []byte, headers map[string]string) error {
		var t T
		err := json.Unmarshal(msg, &t)
		if err != nil {
			return err
		}
		return handler(ctx, t, headers)
	}
	return HandleTopicWithinGroupRaw(h, topic, group)
}

func HandleTopicWithinGroupRaw(handler Handler[[]byte], topic string, group string) shoot.Option[Options, *Options] {
	if handler == nil {
		panic(errx.ErrParamIsNil("handler"))
	}
	return func(o *Options) {
		o.HandlerOptsLst = append(o.HandlerOptsLst, HandlerOptions{
			GroupOptions: GroupOptions{
				Group:             group,
				IngestConcurrency: 0,
			},
			Topic:   topic,
			Handler: handler,
		})
	}
}
