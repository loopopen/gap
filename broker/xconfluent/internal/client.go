package internal

import (
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func clientConfig(opts *Options) kafka.ConfigMap {
	config := make(kafka.ConfigMap, len(opts.Config)+5)
	for key, value := range opts.Config {
		config[key] = value
	}
	config["bootstrap.servers"] = strings.Join(opts.Brokers, ",")
	if opts.UserName != "" || opts.Password != "" {
		config["sasl.username"] = opts.UserName
		config["sasl.password"] = opts.Password
		if _, ok := config["sasl.mechanisms"]; !ok {
			config["sasl.mechanisms"] = "PLAIN"
		}
		if _, ok := config["security.protocol"]; !ok {
			config["security.protocol"] = "SASL_PLAINTEXT"
		}
	}
	return config
}

func consumerConfig(opts *Options, groupID string) (kafka.ConfigMap, error) {
	config := clientConfig(opts)
	config["group.id"] = groupID
	config["enable.auto.commit"] = false
	config["enable.auto.offset.store"] = false

	switch kafka.Offset(opts.StartOffset) {
	case kafka.OffsetBeginning:
		config["auto.offset.reset"] = "earliest"
	case kafka.OffsetEnd:
		config["auto.offset.reset"] = "latest"
	default:
		return nil, fmt.Errorf("confluent: unsupported start offset %d", opts.StartOffset)
	}
	return config, nil
}

var (
	adminOnce sync.Once
	admin     *kafka.AdminClient
	adminErr  error
)

func SingleAdmin(opts *Options) (*kafka.AdminClient, error) {
	adminOnce.Do(func() {
		config := clientConfig(opts)
		admin, adminErr = kafka.NewAdminClient(&config)
	})
	return admin, adminErr
}
