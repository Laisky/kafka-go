package kafka

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

func TestCommitLoopLogsErrorsLiterally(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		fail          bool
	}{
		{"percent signs are data", "offset 100% unavailable: %s", true},
		{"ordinary error", "offset unavailable", true},
		{"successful commit has no error log", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var output bytes.Buffer
			r := &Reader{config: ReaderConfig{CommitInterval: time.Hour, ErrorLogger: log.New(&output, "", 0)}, stctx: ctx, commits: make(chan commitRequest, 1)}
			r.commits <- commitRequest{commits: []commit{{topic: "synthetic", partition: 0, offset: 1}}}
			calls := 0
			gen := &Generation{conn: mockCoordinator{offsetCommitFunc: func(offsetCommitRequestV2) (offsetCommitResponseV2, error) {
				calls++
				if tc.fail {
					return offsetCommitResponseV2{}, errors.New(tc.message)
				}
				return offsetCommitResponseV2{}, nil
			}}, log: func(func(Logger)) {}}
			r.commitLoopInterval(ctx, gen)
			if calls != 1 {
				t.Fatalf("coordinator calls = %d, want 1", calls)
			}
			if got := strings.TrimSuffix(output.String(), "\n"); got != tc.message {
				t.Errorf("error log = %q, want %q", got, tc.message)
			}
		})
	}
}
