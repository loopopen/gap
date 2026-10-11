package internal

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type kafkaConnections struct {
	client    *kafka.Client
	dialer    *kafka.Dialer
	transport *kafka.Transport
}

var connections *kafkaConnections
var connectionsOnce sync.Once

func newKafkaConnections(opts *Options) *kafkaConnections {
	var mechanism sasl.Mechanism
	if opts.UserName != "" && opts.Password != "" {
		mechanism = plain.Mechanism{
			Username: opts.UserName,
			Password: opts.Password,
		}
	}

	var tlsConfig *tls.Config
	if opts.TLSConfig != nil {
		tlsConfig = opts.TLSConfig.Clone()
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
		TLS:         tlsConfig,
		SASL:        mechanism,
	}

	return &kafkaConnections{
		client: &kafka.Client{
			Addr:      kafka.TCP(opts.Brokers...),
			Timeout:   10 * time.Second,
			Transport: transport,
		},
		dialer:    dialer,
		transport: transport,
	}
}

func singleKafkaConnections(opts *Options) *kafkaConnections {
	connectionsOnce.Do(func() {
		connections = newKafkaConnections(opts)
	})
	return connections
}

func SingleClient(opts *Options) *kafka.Client {
	return singleKafkaConnections(opts).client
}

func SingleDialer(opts *Options) *kafka.Dialer {
	return singleKafkaConnections(opts).dialer
}

func SingleTransport(opts *Options) *kafka.Transport {
	return singleKafkaConnections(opts).transport
}
