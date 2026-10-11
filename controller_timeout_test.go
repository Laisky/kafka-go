package kafka

import (
	"context"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/alterpartitionreassignments"
	"github.com/segmentio/kafka-go/protocol/electleaders"
	"net"
	"testing"
	"time"
)

type controllerTimeoutTransport struct {
	t     *testing.T
	want  int32
	calls int
}

func (tr *controllerTimeoutTransport) RoundTrip(ctx context.Context, addr net.Addr, request protocol.Message) (protocol.Message, error) {
	tr.calls++
	switch req := request.(type) {
	case *alterpartitionreassignments.Request:
		if req.TimeoutMs != tr.want {
			tr.t.Errorf("reassignment timeout = %d, want %d", req.TimeoutMs, tr.want)
		}
		if len(req.Topics) != 1 || req.Topics[0].Name != "synthetic" || len(req.Topics[0].Partitions) != 1 || req.Topics[0].Partitions[0].PartitionIndex != 2 || req.Topics[0].Partitions[0].Replicas[0] != 1 {
			tr.t.Fatal("reassignment payload changed")
		}
		return &alterpartitionreassignments.Response{}, nil
	case *electleaders.Request:
		if req.TimeoutMs != tr.want {
			tr.t.Errorf("election timeout = %d, want %d", req.TimeoutMs, tr.want)
		}
		if len(req.TopicPartitions) != 1 || req.TopicPartitions[0].Topic != "synthetic" || req.TopicPartitions[0].PartitionIDs[0] != 2 {
			tr.t.Fatal("election payload changed")
		}
		return &electleaders.Response{}, nil
	default:
		tr.t.Fatalf("unexpected protocol request %T", req)
	}
	return nil, nil
}
func TestControllerRequestTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		request, client time.Duration
		want            int32
	}{
		{"zero gets default", 0, 0, 2000},
		{"zero uses client limit", 0, 8 * time.Second, 4000},
		{"explicit timeout preserved", 7300 * time.Millisecond, 0, 7300},
		{"explicit beats client default", 7300 * time.Millisecond, 8 * time.Second, 7300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &controllerTimeoutTransport{t: t, want: tc.want}
			c := Client{Addr: TCP("synthetic:9092"), Timeout: tc.client, Transport: tr}
			_, err := c.AlterPartitionReassignments(context.Background(), &AlterPartitionReassignmentsRequest{Topic: "synthetic", Timeout: tc.request, Assignments: []AlterPartitionReassignmentsRequestAssignment{{PartitionID: 2, BrokerIDs: []int{1}}}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ElectLeaders(context.Background(), &ElectLeadersRequest{Topic: "synthetic", Timeout: tc.request, Partitions: []int{2}})
			if err != nil {
				t.Fatal(err)
			}
			if tr.calls != 2 {
				t.Fatalf("calls = %d, want 2", tr.calls)
			}
		})
	}
}
