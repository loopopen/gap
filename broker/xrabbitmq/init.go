package xrabbitmq

import (
	"log/slog"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
)

type factory struct{}

func (f *factory) CreateBroker(gapOpts *gap.Options) (broker.Broker, error) {
	brok := NewBroker(gapOpts)
	err := brok.init()
	if err != nil {
		slog.Error("failed to init broker", slog.Any("err", err))
		return nil, err
	}
	return brok, nil
}

func init() {
	internal.Register[broker.Factory](enum.RabbitMQ, &factory{})
	dashboard.AddMeta(enum.Broker, enum.RabbitMQ, version)
}
