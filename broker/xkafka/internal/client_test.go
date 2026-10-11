package internal

import (
	"crypto/tls"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestNewKafkaConnectionsSharesAuthenticationConfiguration(t *testing.T) {
	opts := new(Options).With(
		UserName("alice"),
		Password("secret"),
		TLSConfig(&tls.Config{ServerName: "kafka.example.com", MinVersion: tls.VersionTLS12}),
	)

	connections := newKafkaConnections(opts)

	if connections.client.Transport != connections.transport {
		t.Fatal("admin client does not use the shared transport")
	}
	if connections.dialer.TLS == nil || connections.transport.TLS == nil {
		t.Fatal("TLS config was not applied to both dialer and transport")
	}
	if connections.dialer.TLS != connections.transport.TLS {
		t.Fatal("dialer and transport do not share the same TLS config")
	}
	if connections.dialer.TLS == opts.TLSConfig {
		t.Fatal("caller-owned TLS config was not cloned")
	}

	dialerMechanism, ok := connections.dialer.SASLMechanism.(plain.Mechanism)
	if !ok {
		t.Fatalf("dialer SASL mechanism = %T, want plain.Mechanism", connections.dialer.SASLMechanism)
	}
	transportMechanism, ok := connections.transport.SASL.(plain.Mechanism)
	if !ok {
		t.Fatalf("transport SASL mechanism = %T, want plain.Mechanism", connections.transport.SASL)
	}
	if dialerMechanism.Username != "alice" || dialerMechanism.Password != "secret" {
		t.Fatal("dialer does not contain configured SASL credentials")
	}
	if transportMechanism != dialerMechanism {
		t.Fatal("dialer and transport do not use the same SASL credentials")
	}
}

func TestReaderAndWriterUseAuthenticatedConnections(t *testing.T) {
	opts := new(Options).With(Brokers([]string{"localhost:9092"}))
	connections := newKafkaConnections(opts)

	reader := newKafkaReader(opts, connections.dialer, "test-group", []string{"test-topic"})
	t.Cleanup(func() { _ = reader.Close() })
	if reader.Config().Dialer != connections.dialer {
		t.Fatal("reader does not use the authenticated dialer")
	}

	writer := newKafkaWriter(opts, connections.transport)
	if writer.Transport != connections.transport {
		t.Fatal("writer does not use the authenticated transport")
	}
	if writer.Addr.String() != kafka.TCP(opts.Brokers...).String() {
		t.Fatalf("writer address = %q, want %q", writer.Addr, kafka.TCP(opts.Brokers...))
	}
}

func TestNewKafkaConnectionsWithoutAuthentication(t *testing.T) {
	connections := newKafkaConnections(new(Options).With())

	if connections.dialer.SASLMechanism != nil || connections.transport.SASL != nil {
		t.Fatal("anonymous connection unexpectedly has SASL configured")
	}
	if connections.dialer.TLS != nil || connections.transport.TLS != nil {
		t.Fatal("anonymous connection unexpectedly has TLS configured")
	}
}
