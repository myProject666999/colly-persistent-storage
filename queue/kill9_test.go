package queue

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/storage/persistent"
)

// TestPersistentQueueKill9Recovery runs a worker as a real subprocess,
// sends it SIGKILL while a request is in flight, and verifies that a second
// worker process re-processes the unacknowledged request without losing any
// queued item.
func TestPersistentQueueKill9Recovery(t *testing.T) {
	srv, hits := newPersistentQueueTestServer(t)
	dir := t.TempDir()

	// Enqueue two requests before starting any worker.
	seed := persistent.NewStorage(dir)
	if err := seed.Init(); err != nil {
		t.Fatalf("seed Init() error: %v", err)
	}
	q, err := New(1, seed)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := q.AddURL(srv.URL + "/killed/1"); err != nil {
		t.Fatalf("AddURL() error: %v", err)
	}
	if err := q.AddURL(srv.URL + "/killed/2"); err != nil {
		t.Fatalf("AddURL() error: %v", err)
	}
	seed.Close()

	// Stage 1: worker subprocess kills itself from inside an OnResponse
	// callback while the next request is already dequeued/in flight.
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKill9Worker$")
	cmd.Env = append(os.Environ(),
		"COLLY_KILL9_WORKER=1",
		"COLLY_QUEUE_DIR="+dir,
		"COLLY_TARGET_URL="+srv.URL,
	)
	out, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("worker subprocess was expected to be killed, exited cleanly; output: %s", out)
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "queue", "inflight")); len(entries) == 0 {
		t.Fatalf("expected an inflight request left by the killed process; output: %s", out)
	}

	// Stage 2: a normal worker drains everything remaining.
	store := persistent.NewStorage(dir)
	q2, err := New(1, store)
	if err != nil {
		t.Fatalf("second New() error: %v", err)
	}
	var processed atomic.Int64
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(r *colly.Response) { processed.Add(1) })
	if err := q2.Run(c); err != nil {
		t.Fatalf("recovery Run() error: %v", err)
	}
	if processed.Load() == 0 {
		t.Fatal("no request was re-processed after kill -9")
	}
	// The killed request is fetched again after recovery, so the server
	// observes more hits than the number of distinct URLs.
	if hits.Load() < 2 {
		t.Fatalf("server hits = %d, expected both URLs to be fetched", hits.Load())
	}
	if size, _ := store.QueueSize(); size != 0 {
		t.Fatalf("QueueSize() after recovery = %d, want 0", size)
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "queue", "inflight")); len(entries) != 0 {
		t.Fatalf("inflight not drained: %d entries", len(entries))
	}
	store.Close()
}

// TestKill9Worker is the worker subprocess used by
// TestPersistentQueueKill9Recovery. It SIGKILLs itself after the first
// successful response, leaving the next dequeued request unacknowledged on
// disk.
func TestKill9Worker(t *testing.T) {
	if os.Getenv("COLLY_KILL9_WORKER") != "1" {
		return
	}
	dir := os.Getenv("COLLY_QUEUE_DIR")
	store := persistent.NewStorage(dir)
	q, err := New(1, store)
	if err != nil {
		t.Fatalf("worker New() error: %v", err)
	}
	// Pre-dequeue the second request so it is guaranteed to be in flight
	// when the process dies.
	if _, _, err := store.GetRequestForAck(); err != nil {
		t.Fatalf("worker GetRequestForAck() error: %v", err)
	}
	c := colly.NewCollector()
	c.OnResponse(func(r *colly.Response) {
		_ = os.WriteFile(filepath.Join(dir, "worker-died-"+strconv.Itoa(os.Getpid())), []byte("1"), 0640)
		// Die immediately: no ack for either the running request nor the
		// pre-dequeued one.
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			t.Fatalf("SIGKILL failed: %v", err)
		}
		time.Sleep(time.Second) // unreachable
	})
	// Re-add the pre-dequeued request's URL through the collector's own
	// queue path would block; instead just run with the first ready item.
	if err := q.Run(c); err != nil {
		t.Fatalf("worker Run() error: %v", err)
	}
}
