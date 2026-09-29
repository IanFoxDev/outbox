// Package publish holds the places the relay can send events to.
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// Stdout writes one JSON line per event. It is for trying the relay without Kafka
// and for debugging, not for production.
type Stdout struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStdout writes to w, usually os.Stdout.
func NewStdout(w io.Writer) *Stdout {
	return &Stdout{enc: json.NewEncoder(w)}
}

type line struct {
	ID            int64             `json:"id"`
	EventID       string            `json:"event_id"`
	Source        string            `json:"source"`
	Type          string            `json:"type"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID   string            `json:"aggregate_id"`
	ContentType   string            `json:"content_type"`
	Time          time.Time         `json:"time"`
	Headers       map[string]string `json:"headers,omitempty"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	PayloadBytes  []byte            `json:"payload_base64,omitempty"`
}

// Publish writes the rows and reports every one written as delivered.
func (s *Stdout) Publish(_ context.Context, rows []store.Row) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delivered := make([]int64, 0, len(rows))
	for _, r := range rows {
		l := line{
			ID: r.ID, EventID: r.EventID, Source: r.Source, Type: r.EventType,
			AggregateType: r.AggregateType, AggregateID: r.AggregateID,
			ContentType: r.ContentType, Time: r.CreatedAt, Headers: r.Headers,
		}
		if strings.HasPrefix(r.ContentType, "application/json") && json.Valid(r.Payload) {
			l.Payload = r.Payload
		} else {
			l.PayloadBytes = r.Payload
		}
		if err := s.enc.Encode(l); err != nil {
			return delivered, fmt.Errorf("write event %d: %w", r.ID, err)
		}
		delivered = append(delivered, r.ID)
	}
	return delivered, nil
}
