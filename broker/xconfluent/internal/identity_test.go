package internal

import "testing"

func TestConnectionKeyFollowsBrokerIdentity(t *testing.T) {
	sameA := &Options{Brokers: []string{"b:9092", "a:9092"}, UserName: "u", Password: "p"}
	sameB := &Options{Brokers: []string{"a:9092", "b:9092"}, UserName: "u", Password: "p"}
	if sameA.connectionKey() != sameB.connectionKey() {
		t.Fatal("broker order changed the connection key")
	}
	other := &Options{Brokers: []string{"a:9092"}, UserName: "u", Password: "p"}
	if sameA.connectionKey() == other.connectionKey() {
		t.Fatal("different brokers produced the same connection key")
	}
	if sameA.consumerKey("v1", "g") == sameB.consumerKey("v1", "other") {
		t.Fatal("different groups produced the same consumer key")
	}
	if sameA.consumerKey("v1", "g") == sameA.consumerKey("v2", "g") {
		t.Fatal("different versions produced the same consumer key")
	}
}
