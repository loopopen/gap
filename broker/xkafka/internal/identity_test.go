package internal

import (
	"crypto/tls"
	"testing"
)

func TestKafkaConnectionsFollowBrokerIdentity(t *testing.T) {
	sameA := &Options{
		Brokers:   []string{"b:9092", "a:9092"},
		UserName:  "u",
		Password:  "p",
		TLSConfig: &tls.Config{ServerName: "kafka"},
	}
	sameB := &Options{
		Brokers:   []string{"a:9092", "b:9092"},
		UserName:  "u",
		Password:  "p",
		TLSConfig: sameA.TLSConfig,
	}
	otherBrokers := &Options{Brokers: []string{"a:9092"}, UserName: "u", Password: "p", TLSConfig: sameA.TLSConfig}
	otherPassword := &Options{Brokers: []string{"a:9092", "b:9092"}, UserName: "u", Password: "other", TLSConfig: sameA.TLSConfig}

	if singleKafkaConnections(sameA) != singleKafkaConnections(sameB) {
		t.Fatal("same broker identity opened two connection sets")
	}
	if singleKafkaConnections(sameA) == singleKafkaConnections(otherBrokers) {
		t.Fatal("different brokers shared connections")
	}
	if singleKafkaConnections(sameA) == singleKafkaConnections(otherPassword) {
		t.Fatal("different passwords shared connections")
	}
	if SingleEnsurer(sameA) != SingleEnsurer(sameB) {
		t.Fatal("same broker identity opened two ensurers")
	}
	if SingleEnsurer(sameA) == SingleEnsurer(otherBrokers) {
		t.Fatal("different brokers shared an ensurer")
	}
}
