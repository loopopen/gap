package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

/*
GAP_KAFKA_SASL_BROKERS=127.0.0.1:9092 \
GAP_KAFKA_SASL_USERNAME='kafka' \
GAP_KAFKA_SASL_PASSWORD='kafka' \
go test ./broker/xkafka/internal \
  -run '^TestSASLAuthenticationIntegration$' \
  -count=1 -v
*/

// TestSASLAuthenticationIntegration runs only when GAP_KAFKA_SASL_BROKERS,
// GAP_KAFKA_SASL_USERNAME, and GAP_KAFKA_SASL_PASSWORD are set. Set
// GAP_KAFKA_TLS_CA_FILE (and optionally GAP_KAFKA_TLS_SERVER_NAME) to exercise
// SASL over TLS as well.
func TestSASLAuthenticationIntegration(t *testing.T) {
	brokersValue := os.Getenv("GAP_KAFKA_SASL_BROKERS")
	username := os.Getenv("GAP_KAFKA_SASL_USERNAME")
	password := os.Getenv("GAP_KAFKA_SASL_PASSWORD")
	if brokersValue == "" || username == "" || password == "" {
		t.Skip("SASL Kafka integration environment is not configured")
	}

	var tlsConfig *tls.Config
	if caFile := os.Getenv("GAP_KAFKA_TLS_CA_FILE"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			t.Fatalf("read TLS CA: %v", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			t.Fatalf("load system certificate pool: %v", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			t.Fatal("TLS CA file does not contain a certificate")
		}
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: os.Getenv("GAP_KAFKA_TLS_SERVER_NAME"),
		}
	}

	opts := new(Options).With(
		Brokers(strings.Split(brokersValue, ",")),
		UserName(username),
		Password(password),
		TLSConfig(tlsConfig),
	)
	connections := newKafkaConnections(opts)
	connections.transport.Context = t.Context()
	t.Cleanup(connections.transport.CloseIdleConnections)

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	topic := fmt.Sprintf("gap-auth-integration-%d", time.Now().UnixNano())

	if _, err := connections.client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("authenticated CreateTopics: %v", err)
	}

	writer := newKafkaWriter(opts, connections.transport)
	t.Cleanup(func() { _ = writer.Close() })
	want := []byte("authenticated-message")
	if err := writer.WriteMessages(ctx, kafka.Message{Topic: topic, Value: want}); err != nil {
		t.Fatalf("authenticated produce: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     opts.Brokers,
		Dialer:      connections.dialer,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MaxWait:     time.Second,
	})
	t.Cleanup(func() { _ = reader.Close() })
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("authenticated fetch: %v", err)
	}
	if string(message.Value) != string(want) {
		t.Fatalf("message = %q, want %q", message.Value, want)
	}
}
