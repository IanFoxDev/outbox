package publish

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/store"
)

func TestStdoutWritesOneLinePerEvent(t *testing.T) {
	var buf bytes.Buffer
	created := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	rows := []store.Row{
		{ID: 1, EventID: "e1", Source: "/orders", EventType: "OrderPlaced", AggregateType: "order", AggregateID: "42",
			ContentType: "application/json", Payload: []byte(`{"total":1999}`), CreatedAt: created},
		{ID: 2, EventID: "e2", Source: "/orders", EventType: "OrderPaid", AggregateType: "order", AggregateID: "42",
			ContentType: "application/x-protobuf", Payload: []byte{0x00, 0xff}, CreatedAt: created,
			Headers: map[string]string{"traceparent": "00-abc-def-01"}},
	}

	delivered, err := NewStdout(&buf).Publish(context.Background(), rows)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(delivered, []int64{1, 2}) {
		t.Errorf("delivered %v", delivered)
	}

	want := `{"id":1,"event_id":"e1","source":"/orders","type":"OrderPlaced","aggregate_type":"order","aggregate_id":"42","content_type":"application/json","time":"2026-09-29T12:00:00Z","payload":{"total":1999}}
{"id":2,"event_id":"e2","source":"/orders","type":"OrderPaid","aggregate_type":"order","aggregate_id":"42","content_type":"application/x-protobuf","time":"2026-09-29T12:00:00Z","headers":{"traceparent":"00-abc-def-01"},"payload_base64":"AP8="}
`
	if got := buf.String(); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, strings.TrimSpace(want))
	}
}
