package internal

import (
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var client *kafka.Client
var clientOnce sync.Once

func SingleClient(opts *Options) *kafka.Client {
	clientOnce.Do(func() {
		var t = kafka.DefaultTransport
		if opts.UserName != "" && opts.Password != "" {
			t = &kafka.Transport{
				SASL: plain.Mechanism{
					Username: opts.UserName,
					Password: opts.Password,
				},
			}
		}
		client = &kafka.Client{
			Addr:      kafka.TCP(opts.Brokers...),
			Timeout:   10 * time.Second,
			Transport: t,
		}
	})
	return client
}
