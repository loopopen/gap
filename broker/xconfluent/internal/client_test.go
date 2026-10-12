package internal

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumerConfig(t *testing.T) {
	opts := new(Options).With(
		Brokers([]string{"one:9092", "two:9092"}),
		UserName("user"),
		Password("secret"),
		StartOffset(kafka.OffsetBeginning),
		ClientConfig(kafka.ConfigMap{
			"security.protocol": "SASL_SSL",
			"client.id":         "gap-test",
		}),
	)
	config, err := consumerConfig(opts, "test-group")
	if err != nil {
		t.Fatalf("consumerConfig: %v", err)
	}
	wants := map[string]any{
		"bootstrap.servers":        "one:9092,two:9092",
		"group.id":                 "test-group",
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "PLAIN",
		"sasl.username":            "user",
		"sasl.password":            "secret",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"client.id":                "gap-test",
	}
	for key, want := range wants {
		if got := config[key]; got != want {
			t.Errorf("config[%q] = %#v, want %#v", key, got, want)
		}
	}
}

func TestConsumerConfigRejectsAbsoluteStartOffset(t *testing.T) {
	opts := new(Options).With(StartOffset(12))
	if _, err := consumerConfig(opts, "test-group"); err == nil {
		t.Fatal("consumerConfig accepted an unsupported absolute start offset")
	}
}
