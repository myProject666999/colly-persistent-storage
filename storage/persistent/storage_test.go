package persistent

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gocolly/colly/v2/storage"
)

func newTestStorage(t *testing.T, opts ...Option) *Storage {
	t.Helper()
	dir := t.TempDir()
	s := NewStorage(dir, opts...)
	if err := s.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestVisitedPersistence(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)
	if err := s.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	key := storage.RequestKey("GET", "http://example.com/page", nil)
	if visited, err := s.IsVisitedKey(key); err != nil || visited {
		t.Fatalf("IsVisitedKey before visit = %v, %v; want false, nil", visited, err)
	}
	if err := s.VisitedKey(key); err != nil {
		t.Fatalf("VisitedKey() error: %v", err)
	}
	// In-flight within the same process counts as visited.
	if visited, err := s.IsVisitedKey(key); err != nil || !visited {
		t.Fatalf("IsVisitedKey after visit = %v, %v; want true, nil", visited, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Simulate a process restart: a brand new storage on the same directory.
	s2 := NewStorage(dir)
	if err := s2.Init(); err != nil {
		t.Fatalf("second Init() error: %v", err)
	}
	defer s2.Close()
	// The request never completed (no RecordResponse), so a fresh process
	// must treat it as unvisited and allow the crawl to retry.
	if visited, err := s2.IsVisitedKey(key); err != nil || visited {
		t.Fatalf("IsVisitedKey after restart without completion = %v, %v; want false, nil", visited, err)
	}
}

func TestVisitedKeyCollisionSafety(t *testing.T) {
	// Two distinct request identities. The backend keys records by the full
	// identity string (hashed with SHA-256), never by the truncated 64-bit
	// FNV value colly uses for the legacy interface, so neither can hide the
	// other.
	s := newTestStorage(t)
	key1 := storage.RequestKey("GET", "http://example.com/a", nil)
	key2 := storage.RequestKey("GET", "http://example.com/b", nil)
	if err := s.VisitedKey(key1); err != nil {
		t.Fatalf("VisitedKey(key1) error: %v", err)
	}
	if visited, _ := s.IsVisitedKey(key2); visited {
		t.Fatal("distinct key reported as visited because of shared hash namespace")
	}
	if err := s.VisitedKey(key2); err != nil {
		t.Fatalf("VisitedKey(key2) error: %v", err)
	}
	if visited, _ := s.IsVisitedKey(key1); !visited {
		t.Fatal("key1 lost after key2 was stored")
	}
	if visited, _ := s.IsVisitedKey(key2); !visited {
		t.Fatal("key2 not stored")
	}
	// Physical record files must be different.
	if s.recordPath(key1) == s.recordPath(key2) {
		t.Fatal("collision: distinct keys map to the same record file")
	}
}

func TestLegacyIDs(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)
	if err := s.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if err := s.Visited(42); err != nil {
		t.Fatalf("Visited() error: %v", err)
	}
	if visited, err := s.IsVisited(42); err != nil || !visited {
		t.Fatalf("IsVisited(42) = %v, %v; want true", visited, err)
	}
	if visited, _ := s.IsVisited(43); visited {
		t.Fatal("unknown id reported as visited")
	}
	s.Close()

	s2 := NewStorage(dir)
	if err := s2.Init(); err != nil {
		t.Fatalf("second Init() error: %v", err)
	}
	defer s2.Close()
	if visited, _ := s2.IsVisited(42); !visited {
		t.Fatal("legacy id did not survive restart")
	}
}

func TestIncrementalFreshnessAndMetadata(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir, WithRevisitInterval(50*time.Millisecond))
	if err := s.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if s.RevisitInterval() != 50*time.Millisecond {
		t.Fatalf("RevisitInterval() = %v", s.RevisitInterval())
	}
	key := storage.RequestKey("GET", "http://example.com/news", nil)
	meta := storage.ResponseMeta{
		FetchedAt:    time.Now(),
		ETag:         `"v1"`,
		LastModified: "Mon, 01 Jan 2024 00:00:00 GMT",
		ContentHash:  "abc123",
	}
	if err := s.RecordResponse(key, meta); err != nil {
		t.Fatalf("RecordResponse() error: %v", err)
	}
	if visited, _ := s.IsVisitedKey(key); !visited {
		t.Fatal("completed fetch should be visited while fresh")
	}
	got, err := s.GetResponseMeta(key)
	if err != nil {
		t.Fatalf("GetResponseMeta() error: %v", err)
	}
	if got.ETag != meta.ETag || got.LastModified != meta.LastModified || got.ContentHash != meta.ContentHash {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	// After the interval elapses the URL is stale and must be re-crawled.
	time.Sleep(70 * time.Millisecond)
	if visited, _ := s.IsVisitedKey(key); visited {
		t.Fatal("stale URL still reported as visited")
	}
	// Metadata remains available for conditional requests.
	got, _ = s.GetResponseMeta(key)
	if got.ETag == "" || got.LastModified == "" {
		t.Fatalf("validators lost after expiry: %+v", got)
	}
}

func TestZeroIntervalNeverExpires(t *testing.T) {
	s := newTestStorage(t)
	key := storage.RequestKey("GET", "http://example.com/static", nil)
	if err := s.RecordResponse(key, storage.ResponseMeta{FetchedAt: time.Now(), ContentHash: "h"}); err != nil {
		t.Fatalf("RecordResponse() error: %v", err)
	}
	if visited, _ := s.IsVisitedKey(key); !visited {
		t.Fatal("zero interval should behave like a permanent visited-set")
	}
}

func TestRecordResponseUpdatesMetadata(t *testing.T) {
	s := newTestStorage(t, WithRevisitInterval(time.Hour))
	key := storage.RequestKey("GET", "http://example.com/x", nil)
	if err := s.RecordResponse(key, storage.ResponseMeta{
		FetchedAt:    time.Now().Add(-time.Hour),
		ETag:         `"v1"`,
		LastModified: "Mon, 01 Jan 2024 00:00:00 GMT",
		ContentHash:  "hash1",
	}); err != nil {
		t.Fatalf("RecordResponse() error: %v", err)
	}
	now := time.Now()
	if err := s.RecordResponse(key, storage.ResponseMeta{
		FetchedAt:    now,
		ETag:         `"v2"`,
		LastModified: "Tue, 02 Jan 2024 00:00:00 GMT",
		ContentHash:  "hash2",
	}); err != nil {
		t.Fatalf("second RecordResponse() error: %v", err)
	}
	got, _ := s.GetResponseMeta(key)
	if got.ETag != `"v2"` || got.ContentHash != "hash2" || !got.FetchedAt.Equal(now) {
		t.Fatalf("metadata not updated: %+v", got)
	}
	if visited, _ := s.IsVisitedKey(key); !visited {
		t.Fatal("updated record should be fresh again")
	}
}

func TestCookiesPersistence(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)
	s.Init()
	u, _ := url.Parse("http://example.com/path")
	if got := s.Cookies(u); got != "" {
		t.Fatalf("Cookies() on empty store = %q", got)
	}
	s.SetCookies(u, "session=abc; Path=/")
	s.Close()

	s2 := NewStorage(dir)
	s2.Init()
	defer s2.Close()
	if got := s2.Cookies(u); got != "session=abc; Path=/" {
		t.Fatalf("cookies lost across restart: %q", got)
	}
}

func TestQueueLifecycle(t *testing.T) {
	s := newTestStorage(t)
	size, err := s.QueueSize()
	if err != nil || size != 0 {
		t.Fatalf("empty QueueSize() = %d, %v", size, err)
	}
	if _, _, err := s.GetRequestForAck(); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("GetRequestForAck on empty queue error = %v, want ErrQueueEmpty", err)
	}
	if err := s.AddRequest([]byte("req1")); err != nil {
		t.Fatalf("AddRequest() error: %v", err)
	}
	if err := s.AddRequest([]byte("req2")); err != nil {
		t.Fatalf("AddRequest() error: %v", err)
	}
	if size, _ := s.QueueSize(); size != 2 {
		t.Fatalf("QueueSize() = %d, want 2", size)
	}
	// FIFO order.
	data, id1, err := s.GetRequestForAck()
	if err != nil || string(data) != "req1" {
		t.Fatalf("first GetRequestForAck() = %q, %d, %v", data, id1, err)
	}
	// While in flight the request is no longer "ready".
	if size, _ := s.QueueSize(); size != 1 {
		t.Fatalf("QueueSize() while inflight = %d, want 1", size)
	}
	// Re-dequeue before ack must return the second request, not the first.
	data, id2, _ := s.GetRequestForAck()
	if string(data) != "req2" {
		t.Fatalf("second GetRequestForAck() = %q, want req2", data)
	}
	if err := s.AckRequest(id1); err != nil {
		t.Fatalf("AckRequest() error: %v", err)
	}
	if err := s.AckRequest(id2); err != nil {
		t.Fatalf("AckRequest() error: %v", err)
	}
	if size, _ := s.QueueSize(); size != 0 {
		t.Fatalf("QueueSize() after ack = %d, want 0", size)
	}
	// Duplicate ack is harmless.
	if err := s.AckRequest(id1); err != nil {
		t.Fatalf("duplicate AckRequest() error: %v", err)
	}
}

func TestQueueRecoverySimulatingKill(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)
	s.Init()
	if err := s.AddRequest([]byte("done")); err != nil {
		t.Fatalf("AddRequest() error: %v", err)
	}
	if err := s.AddRequest([]byte("inflight")); err != nil {
		t.Fatalf("AddRequest() error: %v", err)
	}
	// First request processed and acknowledged.
	_, ackDone, err := s.GetRequestForAck()
	if err != nil {
		t.Fatalf("GetRequestForAck() error: %v", err)
	}
	if err := s.AckRequest(ackDone); err != nil {
		t.Fatalf("AckRequest() error: %v", err)
	}
	// Second request dequeued but NOT acknowledged - equivalent to kill -9
	// arriving while the request is being processed.
	data, _, err := s.GetRequestForAck()
	if err != nil {
		t.Fatalf("GetRequestForAck() error: %v", err)
	}
	if string(data) != "inflight" {
		t.Fatalf("dequeued %q, want inflight", data)
	}
	// Verify the payload really sits in the inflight directory on disk.
	entries, err := os.ReadDir(filepath.Join(dir, queueDirName, inflightDir))
	if err != nil {
		t.Fatalf("ReadDir(inflight) error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("inflight entries = %d, want 1", len(entries))
	}
	_ = s.Close()

	// New process opens the same directory: Init must re-queue the request.
	s2 := NewStorage(dir)
	if err := s2.Init(); err != nil {
		t.Fatalf("second Init() error: %v", err)
	}
	defer s2.Close()
	if size, _ := s2.QueueSize(); size != 1 {
		t.Fatalf("QueueSize() after recovery = %d, want 1", size)
	}
	data, ackID, err := s2.GetRequestForAck()
	if err != nil {
		t.Fatalf("recovered GetRequestForAck() error: %v", err)
	}
	if string(data) != "inflight" {
		t.Fatalf("recovered request = %q, want inflight", data)
	}
	// The queue must now be empty; the recovered request is in flight again.
	if _, _, err := s2.GetRequestForAck(); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("queue should be empty after re-dequeue, error = %v", err)
	}
	if err := s2.AckRequest(ackID); err != nil {
		t.Fatalf("AckRequest() error: %v", err)
	}
	entries, _ = os.ReadDir(filepath.Join(dir, queueDirName, inflightDir))
	if len(entries) != 0 {
		t.Fatalf("inflight not drained after ack: %d entries", len(entries))
	}
	if size, _ := s2.QueueSize(); size != 0 {
		t.Fatalf("QueueSize() after ack = %d, want 0", size)
	}
}

func TestQueueAddRequestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := NewStorage(dir)
	s.Init()
	for i := 0; i < 5; i++ {
		if err := s.AddRequest([]byte("r" + string(rune('0'+i)))); err != nil {
			t.Fatalf("AddRequest() error: %v", err)
		}
	}
	s.Close()

	s2 := NewStorage(dir)
	s2.Init()
	defer s2.Close()
	if size, _ := s2.QueueSize(); size != 5 {
		t.Fatalf("QueueSize() after restart = %d, want 5", size)
	}
	for i := 0; i < 5; i++ {
		data, id, err := s2.GetRequestForAck()
		if err != nil {
			t.Fatalf("GetRequestForAck(%d) error: %v", i, err)
		}
		want := "r" + string(rune('0'+i))
		if string(data) != want {
			t.Fatalf("FIFO order broken at %d: got %q, want %q", i, data, want)
		}
		if err := s2.AckRequest(id); err != nil {
			t.Fatalf("AckRequest() error: %v", err)
		}
	}
}

func TestGetRequestAutoAck(t *testing.T) {
	s := newTestStorage(t)
	if err := s.AddRequest([]byte("auto")); err != nil {
		t.Fatalf("AddRequest() error: %v", err)
	}
	data, err := s.GetRequest()
	if err != nil || string(data) != "auto" {
		t.Fatalf("GetRequest() = %q, %v", data, err)
	}
	if size, _ := s.QueueSize(); size != 0 {
		t.Fatalf("QueueSize() after GetRequest = %d, want 0", size)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := newTestStorage(t, WithRevisitInterval(time.Hour))
	const goroutines = 16
	const perG = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := storage.RequestKey("GET", "http://example.com/"+string(rune('a'+g))+"/"+string(rune('a'+i)), nil)
				if err := s.VisitedKey(key); err != nil {
					t.Errorf("VisitedKey() error: %v", err)
					return
				}
				if _, err := s.IsVisitedKey(key); err != nil {
					t.Errorf("IsVisitedKey() error: %v", err)
					return
				}
				if err := s.RecordResponse(key, storage.ResponseMeta{
					FetchedAt:   time.Now(),
					ETag:        `"e"`,
					ContentHash: "h",
				}); err != nil {
					t.Errorf("RecordResponse() error: %v", err)
					return
				}
				payload := []byte(key)
				if err := s.AddRequest(payload); err != nil {
					t.Errorf("AddRequest() error: %v", err)
					return
				}
				if _, _, err := s.GetRequestForAck(); err != nil && !errors.Is(err, ErrQueueEmpty) {
					t.Errorf("GetRequestForAck() error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestInitRequiresDirectory(t *testing.T) {
	s := &Storage{}
	if err := s.Init(); !errors.Is(err, ErrInvalidStorage) {
		t.Fatalf("Init() without Dir error = %v, want ErrInvalidStorage", err)
	}
	if err := s.Close(); !errors.Is(err, ErrInvalidStorage) {
		t.Fatalf("Close() without Init error = %v, want ErrInvalidStorage", err)
	}
}
