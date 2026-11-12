package colly

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocolly/colly/v2/storage"
	"github.com/gocolly/colly/v2/storage/persistent"
)

// newRevalidationServer simulates a site supporting conditional requests.
// It tracks how often each path is actually fetched and lets the test flip
// the served content and validators on demand.
type revalidationServer struct {
	server      *httptest.Server
	fetchHits   atomic.Int64
	notModified atomic.Int64
	condReqs    atomic.Int64
	body        atomic.Value // []byte
	etag        atomic.Value // string
	lastMod     atomic.Value // string
}

func newRevalidationServer(t *testing.T) *revalidationServer {
	t.Helper()
	rs := &revalidationServer{}
	rs.body.Store([]byte("<html><body>version 1</body></html>"))
	rs.etag.Store(`"v1"`)
	rs.lastMod.Store("Mon, 01 Jan 2024 00:00:00 GMT")
	rs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			rs.condReqs.Add(1)
		}
		if r.URL.Path == "/304" {
			rs.fetchHits.Add(1)
			etag := rs.etag.Load().(string)
			if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag {
				rs.notModified.Add(1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		if r.URL.Path == "/post" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("posted"))
			return
		}
		rs.fetchHits.Add(1)
		body := rs.body.Load().([]byte)
		if etag := rs.etag.Load().(string); etag != "" {
			w.Header().Set("ETag", etag)
		}
		if lm := rs.lastMod.Load().(string); lm != "" {
			w.Header().Set("Last-Modified", lm)
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	t.Cleanup(rs.server.Close)
	return rs
}

func newPersistentCollector(t *testing.T, interval time.Duration) (*Collector, *persistent.Storage, string) {
	t.Helper()
	dir := t.TempDir()
	store := persistent.NewStorage(dir, persistent.WithRevisitInterval(interval))
	c := NewCollector()
	if err := c.SetStorage(store); err != nil {
		t.Fatalf("SetStorage() error: %v", err)
	}
	return c, store, dir
}

func TestIncrementalFreshURLSkipped(t *testing.T) {
	rs := newRevalidationServer(t)
	c, store, _ := newPersistentCollector(t, time.Hour)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	if err := c.Visit(rs.server.URL + "/page"); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	// Fresh: the second visit must not hit the network at all.
	err := c.Visit(rs.server.URL + "/page")
	if err == nil {
		t.Fatal("second Visit within freshness window should return AlreadyVisitedError")
	}
	if _, ok := err.(*AlreadyVisitedError); !ok {
		t.Fatalf("error = %T %v, want *AlreadyVisitedError", err, err)
	}
	if got := rs.fetchHits.Load(); got != 1 {
		t.Fatalf("server fetches = %d, want 1", got)
	}
	if responseCalls.Load() != 1 {
		t.Fatalf("OnResponse calls = %d, want 1", responseCalls.Load())
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestIncremental304SkipsCallbacksAndRefreshesRecord(t *testing.T) {
	rs := newRevalidationServer(t)
	c, store, dir := newPersistentCollector(t, 20*time.Millisecond)
	var (
		responseCalls atomic.Int32
		htmlCalls     atomic.Int32
		scrapedCalls  atomic.Int32
	)
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })
	c.OnHTML("body", func(_ *HTMLElement) { htmlCalls.Add(1) })
	c.OnScraped(func(_ *Response) { scrapedCalls.Add(1) })

	url := rs.server.URL + "/304"
	if err := c.Visit(url); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	// Wait for the freshness window to expire, then re-crawl.
	time.Sleep(30 * time.Millisecond)
	if err := c.Visit(url); err != nil {
		t.Fatalf("second Visit() error: %v", err)
	}
	if rs.notModified.Load() != 1 {
		t.Fatalf("304 responses = %d, want 1", rs.notModified.Load())
	}
	if rs.condReqs.Load() == 0 {
		t.Fatal("re-crawl did not send If-None-Match/If-Modified-Since")
	}
	// Callbacks must only run for the first, changed response.
	if responseCalls.Load() != 1 || htmlCalls.Load() != 1 || scrapedCalls.Load() != 1 {
		t.Fatalf("callback counts after 304: response=%d html=%d scraped=%d, all want 1",
			responseCalls.Load(), htmlCalls.Load(), scrapedCalls.Load())
	}
	store.Close()

	// The refreshed record must survive a restart and keep the URL fresh.
	store2 := persistent.NewStorage(dir, persistent.WithRevisitInterval(time.Hour))
	c2 := NewCollector()
	if err := c2.SetStorage(store2); err != nil {
		t.Fatalf("SetStorage() error: %v", err)
	}
	defer store2.Close()
	if err := c2.Visit(url); err == nil {
		t.Fatal("re-crawled 304 URL should stay fresh after restart")
	}
	if rs.notModified.Load() != 1 {
		t.Fatal("server should not be hit again after restart while fresh")
	}
}

func TestIncrementalUnchanged200SkipsCallbacks(t *testing.T) {
	rs := newRevalidationServer(t)
	c, _, _ := newPersistentCollector(t, 20*time.Millisecond)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	url := rs.server.URL + "/page"
	if err := c.Visit(url); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	// Same body, same hash, but the server answers 200 instead of 304.
	if err := c.Visit(url); err != nil {
		t.Fatalf("second Visit() error: %v", err)
	}
	if got := rs.fetchHits.Load(); got != 2 {
		t.Fatalf("server fetches = %d, want 2", got)
	}
	if responseCalls.Load() != 1 {
		t.Fatalf("OnResponse calls = %d, want 1 (unchanged 200 must be suppressed)", responseCalls.Load())
	}
}

func TestIncrementalChangedContentRunsCallbacks(t *testing.T) {
	rs := newRevalidationServer(t)
	c, _, _ := newPersistentCollector(t, 20*time.Millisecond)
	var bodies []string
	var mu sync.Mutex
	c.OnResponse(func(r *Response) {
		mu.Lock()
		bodies = append(bodies, string(r.Body))
		mu.Unlock()
	})

	url := rs.server.URL + "/page"
	if err := c.Visit(url); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	// Change the content and the validators.
	rs.body.Store([]byte("<html><body>version 2 with new content</body></html>"))
	rs.etag.Store(`"v2"`)
	time.Sleep(30 * time.Millisecond)
	if err := c.Visit(url); err != nil {
		t.Fatalf("second Visit() error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("OnResponse calls = %d, want 2", len(bodies))
	}
	if bodies[1] != "<html><body>version 2 with new content</body></html>" {
		t.Fatalf("second body = %q", bodies[1])
	}
}

func TestIncrementalConditionalHeaders(t *testing.T) {
	rs := newRevalidationServer(t)
	c, _, _ := newPersistentCollector(t, 10*time.Millisecond)
	url := rs.server.URL + "/page"
	if err := c.Visit(url); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := c.Visit(url); err != nil {
		t.Fatalf("second Visit() error: %v", err)
	}
	// The server counts requests carrying either conditional header; the
	// revalidation request must carry the stored ETag and Last-Modified.
	if rs.condReqs.Load() != 1 {
		t.Fatalf("conditional requests = %d, want 1", rs.condReqs.Load())
	}
}

func TestIncrementalPostBodiesAreDistinct(t *testing.T) {
	rs := newRevalidationServer(t)
	c, store, _ := newPersistentCollector(t, time.Hour)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	if err := c.Post(rs.server.URL+"/post", map[string]string{"a": "1"}); err != nil {
		t.Fatalf("first Post() error: %v", err)
	}
	// Same URL with a different body is a different request identity.
	if err := c.Post(rs.server.URL+"/post", map[string]string{"a": "2"}); err != nil {
		t.Fatalf("second Post() error: %v", err)
	}
	// Identical POST must be skipped while fresh.
	err := c.Post(rs.server.URL+"/post", map[string]string{"a": "1"})
	if _, ok := err.(*AlreadyVisitedError); !ok {
		t.Fatalf("duplicate POST error = %v, want AlreadyVisitedError", err)
	}
	if responseCalls.Load() != 2 {
		t.Fatalf("OnResponse calls = %d, want 2", responseCalls.Load())
	}
	store.Close()
}

func TestIncrementalHeadNotTrackedAsGet(t *testing.T) {
	rs := newRevalidationServer(t)
	c, _, _ := newPersistentCollector(t, time.Hour)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	url := rs.server.URL + "/page"
	if err := c.Head(url); err != nil {
		t.Fatalf("Head() error: %v", err)
	}
	// A HEAD must not mark the GET identity as visited.
	if err := c.Visit(url); err != nil {
		t.Fatalf("GET after HEAD error: %v", err)
	}
	if responseCalls.Load() != 2 {
		t.Fatalf("OnResponse calls = %d, want 2 (HEAD + GET)", responseCalls.Load())
	}
}

func TestIncrementalRedirectKeyedByOriginalURL(t *testing.T) {
	var redirectHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		redirectHits.Add(1)
		http.Redirect(w, r, "/destination", http.StatusFound)
	})
	mux.HandleFunc("/destination", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"d1"`)
		w.Write([]byte("destination page"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c, _, _ := newPersistentCollector(t, 20*time.Millisecond)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	startURL := srv.URL + "/start"
	if err := c.Visit(startURL); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	// Re-crawling the original URL must follow the redirect again and
	// revalidate the destination instead of being blocked by the redirect
	// hop's visited mark.
	if err := c.Visit(startURL); err != nil {
		t.Fatalf("second Visit() error: %v", err)
	}
	if redirectHits.Load() != 2 {
		t.Fatalf("redirect hits = %d, want 2", redirectHits.Load())
	}
	// The destination body did not change: callbacks run only once.
	if responseCalls.Load() != 1 {
		t.Fatalf("OnResponse calls = %d, want 1", responseCalls.Load())
	}
}

func TestKeyedStorageKeepsDistinctURLsIndependent(t *testing.T) {
	// Two different URLs on the same host. Because the persistent backend
	// stores full request identities (method + normalized URL + body hash)
	// instead of truncated 64-bit ids, visiting the first URL can never make
	// the second URL look visited.
	dir := t.TempDir()
	store := persistent.NewStorage(dir, persistent.WithRevisitInterval(time.Hour))
	c := NewCollector()
	if err := c.SetStorage(store); err != nil {
		t.Fatalf("SetStorage() error: %v", err)
	}
	defer store.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	var hits atomic.Int32
	c.OnResponse(func(r *Response) { hits.Add(1) })
	if err := c.Visit(srv.URL + "/first"); err != nil {
		t.Fatalf("first Visit() error: %v", err)
	}
	if err := c.Visit(srv.URL + "/second"); err != nil {
		t.Fatalf("second distinct URL must not be hidden: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("OnResponse calls = %d, want 2", hits.Load())
	}
}

func TestContentHashIsSHA256(t *testing.T) {
	sum := sha256.Sum256([]byte("abc"))
	if got := sha256HexBytes([]byte("abc")); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha256HexBytes = %q", got)
	}
}

func TestIncrementalErrorStatusKeepsNormalBehavior(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("missing"))
	}))
	defer srv.Close()

	c, store, dir := newPersistentCollector(t, 20*time.Millisecond)
	var errors atomic.Int32
	var responseCalls atomic.Int32
	c.OnError(func(r *Response, err error) { errors.Add(1) })
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })

	url := srv.URL + "/missing"
	if err := c.Visit(url); err == nil {
		t.Fatal("404 should surface as an error")
	}
	if errors.Load() != 1 {
		t.Fatalf("OnError calls = %d, want 1", errors.Load())
	}
	store.Close()

	// A new process (new Storage on the same directory) must be able to
	// retry the failed URL: the failed response never produced a completed
	// record, so its zero-FetchedAt mark cannot block re-crawling.
	store2 := persistent.NewStorage(dir, persistent.WithRevisitInterval(20*time.Millisecond))
	c2 := NewCollector()
	if err := c2.SetStorage(store2); err != nil {
		t.Fatalf("SetStorage() error: %v", err)
	}
	c2.OnError(func(r *Response, err error) { errors.Add(1) })
	c2.OnResponse(func(r *Response) { responseCalls.Add(1) })
	if err := c2.Visit(url); err == nil {
		t.Fatal("second 404 should also surface as an error")
	}
	defer store2.Close()
	if errors.Load() != 2 {
		t.Fatalf("OnError calls = %d, want 2", errors.Load())
	}
	if responseCalls.Load() != 0 {
		t.Fatalf("OnResponse calls = %d, want 0", responseCalls.Load())
	}
	if hits.Load() != 2 {
		t.Fatalf("server hits = %d, want 2", hits.Load())
	}

	meta, err := store2.GetResponseMeta(storage.RequestKey("GET", normalizeURL(url), nil))
	if err != nil {
		t.Fatalf("GetResponseMeta() error: %v", err)
	}
	if !meta.FetchedAt.IsZero() {
		t.Fatalf("failed response must not be recorded as fetched: %+v", meta)
	}
}

func TestIncremental304WithoutPriorRecordFallsThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()
	c, _, _ := newPersistentCollector(t, time.Hour)
	var responseCalls atomic.Int32
	c.OnResponse(func(r *Response) { responseCalls.Add(1) })
	// No prior record: the 304 must not be swallowed as a "revalidation".
	if err := c.Visit(srv.URL + "/orphan304"); err == nil {
		t.Fatal("304 without a prior record should keep the default error behavior")
	}
	if responseCalls.Load() != 0 {
		t.Fatalf("OnResponse calls = %d, want 0", responseCalls.Load())
	}
}
