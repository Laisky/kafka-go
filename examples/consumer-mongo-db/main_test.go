package main

import (
	"bytes"
	"context"
	"errors"
	kafka "github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func isolatedFixtures(t *testing.T) (string, string) {
	t.Helper()
	broker := os.Getenv("KAFKA_GO_TEST_BROKER")
	uri := os.Getenv("KAFKA_GO_TEST_MONGO_URI")
	host, _, err := net.SplitHostPort(broker)
	if err != nil || (host != "localhost" && !net.ParseIP(host).IsLoopback()) {
		t.Fatal("explicit loopback Kafka fixture is required")
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme != "mongodb" || parsed.User != nil || (parsed.Hostname() != "localhost" && !net.ParseIP(parsed.Hostname()).IsLoopback()) {
		t.Fatal("explicit credential-free loopback Mongo fixture is required")
	}
	return broker, uri
}

func TestSyntheticKafkaMessageStoredInMongo(t *testing.T) {
	broker, uri := isolatedFixtures(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic := "mongo-qualification-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	defer conn.DeleteTopics(topic)
	collection := getMongoCollection(uri, "kafka_go_synthetic", topic)
	defer collection.Database().Client().Disconnect(context.Background())
	defer collection.Drop(context.Background())
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, BatchSize: 1}
	payload := []byte(`{"qualification":"synthetic"}`)
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte("synthetic-key"), Value: payload}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader := getKafkaReader(broker, topic, "qualification-"+topic)
	defer reader.Close()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Topic != topic || !bytes.Equal(msg.Value, payload) {
		t.Fatalf("unexpected consumed record: topic=%q value=%q", msg.Topic, msg.Value)
	}
	result, err := collection.InsertOne(ctx, msg)
	if err != nil || result.InsertedID == nil {
		t.Fatalf("insert = %v, %v", result, err)
	}
	var stored struct {
		Topic string
		Value []byte
		Key   []byte
	}
	if err := collection.FindOne(ctx, bson.M{"topic": topic}).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Topic != topic || !bytes.Equal(stored.Value, payload) || string(stored.Key) != "synthetic-key" {
		t.Fatalf("stored record differs: %+v", stored)
	}
	if err := collection.FindOne(ctx, bson.M{"topic": "never-published-synthetic-control"}).Err(); !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("missing-record control = %v", err)
	}
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil || count != 1 {
		t.Fatalf("stored count = %d, %v; want 1", count, err)
	}
}

func TestInvalidMongoURIRejected(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestInvalidMongoURIChild$")
	cmd.Env = append(os.Environ(), "KAFKA_GO_BAD_URI_CHILD=1")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), "scheme") {
		t.Fatalf("invalid URI acceptance: exit=%v output=%s", err, output)
	}
}
func TestInvalidMongoURIChild(t *testing.T) {
	if os.Getenv("KAFKA_GO_BAD_URI_CHILD") != "1" {
		return
	}
	getMongoCollection("synthetic-invalid-scheme://localhost", "synthetic", "records")
	t.Fatal("invalid Mongo URI accepted")
}
