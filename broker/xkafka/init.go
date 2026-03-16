package xkafka

import (
	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
)

type factory struct{}

func (f *factory) CreateBroker(gapOpts *gap.Options) (broker.Broker, error) {
	brok := NewBroker(gapOpts)
	return brok, nil
}

func init() {
	internal.Register[broker.Factory](enum.Kafka, &factory{})
	dashboard.AddMeta(enum.Broker, enum.Kafka, version)
}
