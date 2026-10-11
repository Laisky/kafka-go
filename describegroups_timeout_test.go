package kafka

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go/protocol/describegroups"
	"testing"
)

func TestDescribeGroupsJoinedResults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		failedIndex int
	}{
		{"success", nil, -1},
		{"deadline before success", context.DeadlineExceeded, 0},
		{"cancellation after success", context.Canceled, 1},
		{"transport error after success", errors.New("synthetic transport failure"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			promises := make([]promise, 2)
			requests := make([]Request, 2)
			for i, group := range []string{"first", "second"} {
				requests[i] = &describegroups.Request{Groups: []string{group}}
				if i == tc.failedIndex {
					promises[i] = reject(tc.err)
					continue
				}
				result := make(async, 1)
				result.resolve(&describegroups.Response{Groups: []describegroups.ResponseGroup{{GroupID: group, GroupState: "Stable"}}})
				promises[i] = result
			}
			response, err := join(promises, requests, &describegroups.Response{}).await(context.Background())
			if tc.err != nil {
				if !errors.Is(err, tc.err) || response != nil {
					t.Fatalf("response = %v, error = %v; want original %v", response, err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			groups := response.(*describegroups.Response).Groups
			if len(groups) != 2 || groups[0].GroupID != "first" || groups[1].GroupID != "second" || groups[0].GroupState != "Stable" || groups[1].GroupState != "Stable" {
				t.Fatalf("successful group payload changed: %+v", groups)
			}
		})
	}
}
