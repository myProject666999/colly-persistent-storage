package queue

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/storage/persistent"
)

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q) error: %v", raw, err)
	}
	return u
}

func newPersistentQueueTestServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestPersistentQueueBasicRun(t *testing.T) {
	srv, hits := newPersistentQueueTestServer(t)
	store := persistent.NewStorage(t.TempDir())
	q, err := New(4, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		if err := q.AddURL(fmt.Sprintf("%s/page/%d", srv.URL, i)); err != nil {
			t.Fatalf("AddURL() error: %v", err)
		}
	}
	c := colly.NewCollector()
	var processed atomic.Int64
	c.OnResponse(func(r *colly.Response) { processed.Add(1) })
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if processed.Load() != n || hits.Load() != n {
		t.Fatalf("processed = %d, server hits = %d, want %d", processed.Load(), hits.Load(), n)
	}
	if size, _ := store.QueueSize(); size != 0 {
		t.Fatalf("QueueSize() after run = %d, want 0", size)
	}
}

func TestPersistentQueueSurvivesRestart(t *testing.T) {
	srv, _ := newPersistentQueueTestServer(t)
	dir := t.TempDir()

	// First "process": enqueue requests but only consume part of them.
	store := persistent.NewStorage(dir)
	q, err := New(1, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	const total = 6
	for i := 0; i < total; i++ {
		if err := q.AddURL(fmt.Sprintf("%s/restart/%d", srv.URL, i)); err != nil {
			t.Fatalf("AddURL() error: %v", err)
		}
	}
	// Dequeue exactly two requests via the backend but never run the queue:
	// the first is acknowledged, the second stays in flight.
	_, ack1, err := store.GetRequestForAck()
	if err != nil {
		t.Fatalf("GetRequestForAck() error: %v", err)
	}
	if err := store.AckRequest(ack1); err != nil {
		t.Fatalf("AckRequest() error: %v", err)
	}
	if _, _, err := store.GetRequestForAck(); err != nil {
		t.Fatalf("second GetRequestForAck() error: %v", err)
	}
	ready, _ := store.QueueSize()
	if ready != total-2 {
		t.Fatalf("ready before restart = %d, want %d", ready, total-2)
	}
	store.Close()

	// Second process opens the same directory. Init must move the inflight
	// request back, leaving total-1 requests to process.
	store2 := persistent.NewStorage(dir)
	q2, err := New(2, store2)
	if err != nil {
		t.Fatalf("second New() error: %v", err)
	}
	if size, _ := store2.QueueSize(); size != total-1 {
		t.Fatalf("QueueSize() after recovery = %d, want %d", size, total-1)
	}
	c := colly.NewCollector()
	var processed atomic.Int64
	c.OnResponse(func(r *colly.Response) { processed.Add(1) })
	if err := q2.Run(c); err != nil {
		t.Fatalf("second Run() error: %v", err)
	}
	if processed.Load() != total-1 {
		t.Fatalf("processed after restart = %d, want %d", processed.Load(), total-1)
	}
}

func TestPersistentQueueAckAfterProcessing(t *testing.T) {
	srv, _ := newPersistentQueueTestServer(t)
	dir := t.TempDir()
	store := persistent.NewStorage(dir)
	q, err := New(2, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	q.AddURL(srv.URL + "/ack")

	var inCallback atomic.Bool
	c := colly.NewCollector()
	c.OnResponse(func(r *colly.Response) {
		inCallback.Store(true)
		// While the callback runs the request must still be in flight on
		// disk: a kill here would require a re-run.
		entries, _ := readDirCount(filepath.Join(dir, "queue", "inflight"))
		if entries == 0 {
			t.Error("request acknowledged before OnResponse finished")
		}
	})
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if !inCallback.Load() {
		t.Fatal("OnResponse never ran")
	}
	entries, _ := readDirCount(filepath.Join(dir, "queue", "inflight"))
	if entries != 0 {
		t.Fatalf("inflight entries after Run = %d, want 0", entries)
	}
}

func TestPersistentQueueProcessesAllRequests(t *testing.T) {
	srv, _ := newPersistentQueueTestServer(t)
	store := persistent.NewStorage(t.TempDir())
	q, err := New(4, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if err := q.AddURL(fmt.Sprintf("%s/fifo/%d", srv.URL, i)); err != nil {
			t.Fatalf("AddURL() error: %v", err)
		}
	}
	seen := make(map[int32]bool)
	var seenMu sync.Mutex
	c := colly.NewCollector()
	c.OnResponse(func(r *colly.Response) {
		var idx int32
		fmt.Sscanf(r.Request.URL.Path, "/fifo/%d", &idx)
		seenMu.Lock()
		defer seenMu.Unlock()
		if seen[idx] {
			t.Errorf("request %d processed twice", idx)
		}
		seen[idx] = true
	})
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != n {
		t.Fatalf("processed %d unique requests, want %d: %v", len(seen), n, seen)
	}
	for i := 0; i < n; i++ {
		if !seen[int32(i)] {
			t.Fatalf("request %d was never processed", i)
		}
	}
}

func TestPersistentQueueConcurrentAddAndRun(t *testing.T) {
	srv, hits := newPersistentQueueTestServer(t)
	store := persistent.NewStorage(t.TempDir())
	q, err := New(8, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	// Seed the first item so Run cannot observe an empty queue and exit
	// before the producer goroutine starts adding.
	if err := q.AddURL(srv.URL + "/concurrent?i=0"); err != nil {
		t.Fatalf("seed AddURL() error: %v", err)
	}
	c := colly.NewCollector(colly.AllowURLRevisit())
	var processed atomic.Int64
	c.OnResponse(func(r *colly.Response) { processed.Add(1) })

	done := make(chan struct{})
	go func() {
		for i := 1; i < 100; i++ {
			q.AddURL(fmt.Sprintf("%s/concurrent?i=%d", srv.URL, i))
			time.Sleep(time.Microsecond)
		}
		close(done)
	}()
	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	<-done
	if processed.Load() != 100 || hits.Load() != 100 {
		t.Fatalf("processed = %d, hits = %d, want 100", processed.Load(), hits.Load())
	}
}

func readDirCount(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func TestPersistentQueuePreservesPOSTBodyAndHead(t *testing.T) {
	var gotBodies []string
	var gotMethods []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotMethods = append(gotMethods, r.Method)
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			gotBodies = append(gotBodies, string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := persistent.NewStorage(t.TempDir())
	q, err := New(2, store)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	c := colly.NewCollector(colly.AllowURLRevisit())

	// Serialize a POST and a HEAD request exactly the way the queue does.
	postReq := &colly.Request{
		Method:  "POST",
		URL:     mustParseURL(t, srv.URL+"/submit"),
		Headers: &http.Header{},
		Ctx:     colly.NewContext(),
	}
	postReq.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
	body := strings.NewReader("name=alice&role=admin")
	postReq.Body = body
	postData, err := postReq.Marshal()
	if err != nil {
		t.Fatalf("POST Marshal() error: %v", err)
	}
	headReq := &colly.Request{
		Method:  "HEAD",
		URL:     mustParseURL(t, srv.URL+"/check"),
		Headers: &http.Header{},
		Ctx:     colly.NewContext(),
	}
	headData, err := headReq.Marshal()
	if err != nil {
		t.Fatalf("HEAD Marshal() error: %v", err)
	}
	if err := store.AddRequest(postData); err != nil {
		t.Fatalf("AddRequest(post) error: %v", err)
	}
	if err := store.AddRequest(headData); err != nil {
		t.Fatalf("AddRequest(head) error: %v", err)
	}

	if err := q.Run(c); err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gotMethods) != 2 {
		t.Fatalf("server requests = %v, want 2", gotMethods)
	}
	foundPost, foundHead := false, false
	for _, m := range gotMethods {
		switch m {
		case "POST":
			foundPost = true
		case "HEAD":
			foundHead = true
		}
	}
	if !foundPost || !foundHead {
		t.Fatalf("methods missing: %v", gotMethods)
	}
	if len(gotBodies) != 1 || gotBodies[0] != "name=alice&role=admin" {
		t.Fatalf("POST bodies = %v, want single preserved body", gotBodies)
	}
}
