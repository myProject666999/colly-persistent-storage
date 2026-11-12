package queue

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gocolly/colly/v2"
)

// minimalQueueStorage implements ONLY the original queue.Storage interface,
// emulating a third-party backend. It must not be treated as AckableStorage
// and the queue must keep functioning through GetRequest.
type minimalQueueStorage struct {
	mu    sync.Mutex
	items [][]byte
}

func (m *minimalQueueStorage) Init() error { return nil }
func (m *minimalQueueStorage) AddRequest(r []byte) error {
	m.mu.Lock()
	m.items = append(m.items, r)
	m.mu.Unlock()
	return nil
}
func (m *minimalQueueStorage) GetRequest() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.items) == 0 {
		return nil, nil
	}
	r := m.items[0]
	m.items = m.items[1:]
	return r, nil
}
func (m *minimalQueueStorage) QueueSize() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.items), nil
}

func TestThirdPartyQueueStorageStillWorks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := &minimalQueueStorage{}
	q, err := New(2, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := q.AddURL(srv.URL + "/compat/" + string(rune('a'+i))); err != nil {
			t.Fatalf("AddURL() error: %v", err)
		}
	}
	var processed int
	var mu sync.Mutex
	c := colly.NewCollector()
	c.OnResponse(func(r *colly.Response) {
		mu.Lock()
		processed++
		mu.Unlock()
	})
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if processed != 5 {
		t.Fatalf("processed = %d, want 5", processed)
	}
}
