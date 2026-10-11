package kafka

import (
	"context"
	"testing"
	"time"
)

func TestConnReadByteStreamAndDeadline(t *testing.T) {
	client, topic, shutdown := newLocalClientAndTopic()
	defer shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	reader, err := DialLeader(ctx, "tcp", "localhost:9092", topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if n, err := reader.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty read = %d, %v", n, err)
	}
	writer := Writer{RequiredAcks: RequireAll, Addr: client.Addr, Topic: topic, BatchSize: 1, Transport: client.Transport}
	defer writer.Close()
	for i := 0; i < 32; i++ {
		if err := writer.WriteMessages(ctx, Message{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reader.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		value := []byte{255}
		n, err := reader.Read(value)
		if err != nil || n != 1 || value[0] != byte(i) {
			t.Fatalf("stream record %d = %d %v %v", i, n, value, err)
		}
	}
	if err := reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(make([]byte, 1)); err == nil {
		t.Fatal("empty stream ignored read deadline")
	}
}
