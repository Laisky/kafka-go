package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestConnFetchErrorPreservesNextResponse(t *testing.T) {
	for _, tc := range []struct {
		name         string
		version      apiVersion
		topLevel     bool
		emptyPayload bool
	}{
		{"v2 partition error", v2, false, false},
		{"v5 partition error", v5, false, false},
		{"v10 partition error", v10, false, false},
		{"v10 top-level error", v10, true, false},
		{"v2 empty watermark with record bytes", v2, false, true},
		{"v5 empty watermark with record bytes", v5, false, true},
		{"v10 empty watermark with record bytes", v10, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			conn := NewConn(client, "synthetic", 0)
			defer conn.Close()
			conn.apiVersions.Store(apiVersionMap{fetch: {MinVersion: int16(tc.version), MaxVersion: int16(tc.version)}})
			if _, err := conn.Seek(0, SeekAbsolute|SeekDontCheck); err != nil {
				t.Fatal(err)
			}
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			server.SetDeadline(time.Now().Add(2 * time.Second))
			done := make(chan error, 1)
			go func() {
				for i := 0; i < 2; i++ {
					var size int32
					if err := binary.Read(server, binary.BigEndian, &size); err != nil {
						done <- err
						return
					}
					request := make([]byte, size)
					if _, err := io.ReadFull(server, request); err != nil {
						done <- err
						return
					}
					if len(request) < 8 || apiKey(binary.BigEndian.Uint16(request[0:2])) != fetch || apiVersion(binary.BigEndian.Uint16(request[2:4])) != tc.version {
						done <- fmt.Errorf("unexpected fetch request header")
						return
					}
					code := int16(0)
					if i == 0 && !tc.emptyPayload {
						code = int16(RequestTimedOut)
					}
					body := syntheticFetchHeader(tc.version, tc.topLevel && i == 0, code, tc.emptyPayload && i == 0)
					var response bytes.Buffer
					binary.Write(&response, binary.BigEndian, int32(4+len(body)))
					response.Write(request[4:8])
					response.Write(body)
					if _, err := server.Write(response.Bytes()); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			batch := conn.ReadBatchWith(ReadBatchConfig{MinBytes: 1, MaxBytes: 1024, MaxWait: 10 * time.Millisecond})
			_, err := batch.ReadMessage()
			if !errors.Is(err, RequestTimedOut) {
				t.Fatalf("first fetch error = %v, want broker timeout", err)
			}
			if err := batch.Close(); !errors.Is(err, RequestTimedOut) {
				t.Fatalf("batch error = %v", err)
			}
			if offset, _ := conn.Offset(); offset != 0 {
				t.Fatalf("failed fetch advanced offset to %d", offset)
			}
			batch = conn.ReadBatchWith(ReadBatchConfig{MinBytes: 1, MaxBytes: 1024, MaxWait: 10 * time.Millisecond})
			message, err := batch.ReadMessage()
			if err != nil || string(message.Value) != "healthy" || message.Offset != 0 || message.Topic != "synthetic" {
				t.Fatalf("healthy response after broker error = %+v, %v", message, err)
			}
			if err := batch.Close(); err != nil {
				t.Fatalf("healthy batch close = %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func syntheticFetchHeader(version apiVersion, topLevel bool, code int16, empty bool) []byte {
	var body bytes.Buffer
	put := func(value interface{}) {
		if err := binary.Write(&body, binary.BigEndian, value); err != nil {
			panic(err)
		}
	}
	put(int32(0)) // throttle
	if version >= v10 {
		topCode := int16(0)
		if topLevel {
			topCode = code
		}
		put(topCode)
		put(int32(0)) // session
		if topLevel {
			put(int32(0))
			return body.Bytes()
		}
	}
	put(int32(1)) // topic count
	put(int16(len("synthetic")))
	body.WriteString("synthetic")
	put(int32(1)) // partition count
	put(int32(0)) // partition
	put(code)
	watermark := int64(0)
	if code == 0 && !empty {
		watermark = 1
	}
	put(watermark) // high watermark
	if version >= v5 {
		put(int64(0))  // last stable offset
		put(int64(0))  // log start
		put(int32(-1)) // no aborted transactions
	}
	if version == v2 && code != 0 {
		put(int32(4))
		put(int32(0)) // ignored error-response record bytes
	} else if code != 0 {
		put(int32(0))
	} else {
		record, err := newRecordBatch(nil, Message{Value: []byte("healthy"), Time: time.Unix(100, 0)})
		if err != nil {
			panic(err)
		}
		var records bytes.Buffer
		record.writeTo(&writeBuffer{w: &records})
		body.Write(records.Bytes())
	}
	return body.Bytes()
}
