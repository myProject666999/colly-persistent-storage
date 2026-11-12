package colly

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
)

// minimalCollectorStorage implements ONLY the original storage.Storage
// interface (no KeyedStorage, no IncrementalStorage). It emulates a
// third-party backend written against the published interface and must keep
// compiling and working when the optional extensions are present.
type minimalCollectorStorage struct {
	mu      sync.Mutex
	visited map[uint64]bool
	cookies map[string]string
}

func (m *minimalCollectorStorage) Init() error {
	m.visited = make(map[uint64]bool)
	m.cookies = make(map[string]string)
	return nil
}
func (m *minimalCollectorStorage) Visited(requestID uint64) error {
	m.mu.Lock()
	m.visited[requestID] = true
	m.mu.Unlock()
	return nil
}
func (m *minimalCollectorStorage) IsVisited(requestID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.visited[requestID], nil
}
func (m *minimalCollectorStorage) Cookies(u *url.URL) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cookies[u.Host]
}
func (m *minimalCollectorStorage) SetCookies(u *url.URL, cookies string) {
	m.mu.Lock()
	m.cookies[u.Host] = cookies
	m.mu.Unlock()
}
func (m *minimalCollectorStorage) Close() error { return nil }

func TestThirdPartyStorageStillWorks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := NewCollector()
	store := &minimalCollectorStorage{}
	if err := c.SetStorage(store); err != nil {
		t.Fatalf("SetStorage() error: %v", err)
	}
	var hits int
	c.OnResponse(func(r *Response) { hits++ })
	if err := c.Visit(srv.URL); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	if err := c.Visit(srv.URL); err == nil {
		t.Fatal("second Visit must be rejected as already visited")
	}
	if hits != 1 {
		t.Fatalf("hits = %d, want 1", hits)
	}
}
