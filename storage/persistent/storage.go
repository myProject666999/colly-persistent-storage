// Package persistent provides file-system backed implementations of
// colly's storage.Storage and queue.Storage interfaces.
//
// All data is stored in a local directory and survives process restarts;
// no external service is required. A single Storage value can be used both
// as a Collector storage (via SetStorage) and as a queue storage (via
// queue.New).
//
// The backend additionally implements storage.KeyedStorage and
// storage.IncrementalStorage, which enable collision-safe visited tracking
// and incremental re-crawling when used with a colly.Collector, and
// queue.AckableStorage, which makes queue requests survive a process kill
// between dequeue and completion.
package persistent

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gocolly/colly/v2/storage"
)

const (
	visitedDirName = "visited"
	cookieDirName  = "cookies"
	queueDirName   = "queue"
	readyDirName   = "ready"
	inflightDir    = "inflight"
)

// ErrInvalidStorage is returned when the storage is used without a
// directory or before Init has been called.
var ErrInvalidStorage = errors.New("persistent storage is not initialized")

// Compile-time guarantees that Storage satisfies every interface it claims.
var (
	_ storage.Storage            = (*Storage)(nil)
	_ storage.KeyedStorage       = (*Storage)(nil)
	_ storage.IncrementalStorage = (*Storage)(nil)
	_ queueStorage               = (*Storage)(nil)
)

// queueStorage mirrors queue.Storage without importing the queue package
// (which would create an import cycle).
type queueStorage interface {
	Init() error
	AddRequest([]byte) error
	GetRequest() ([]byte, error)
	QueueSize() (int, error)
}

// Storage is a file-system backed, process-safe persistent storage.
//
// The zero value is not usable; create one with NewStorage. A Storage
// should be closed with Close once the crawl is finished.
type Storage struct {
	// Dir is the directory holding all persistent data. It is created
	// automatically on Init.
	Dir string
	// RevisitAfter defines how long a successfully fetched URL is
	// considered fresh. While fresh, the Collector skips the URL; once the
	// interval elapses the URL is re-fetched conditionally (If-None-Match /
	// If-Modified-Since). A zero value means URLs never expire and the
	// backend behaves like a plain persistent visited-set.
	RevisitAfter time.Duration
	// FileMode is used for newly created files. Zero defaults to 0640.
	FileMode os.FileMode
	// DirMode is used for newly created directories. Zero defaults to 0750.
	DirMode os.FileMode

	mu       sync.RWMutex
	inited   bool
	inflight map[string]struct{} // request keys dequeued/visited in this process but not completed
	legacy   *legacyStore        // 64-bit FNV request ids for the storage.Storage interface
	seq      *queueSeq           // monotonic queue file sequence counter
}

// Option configures a Storage at construction time.
type Option func(*Storage)

// WithRevisitInterval configures the incremental re-crawl interval.
func WithRevisitInterval(d time.Duration) Option {
	return func(s *Storage) {
		s.RevisitAfter = d
	}
}

// WithFileMode overrides the default file permission (0640).
func WithFileMode(m os.FileMode) Option {
	return func(s *Storage) {
		s.FileMode = m
	}
}

// WithDirMode overrides the default directory permission (0750).
func WithDirMode(m os.FileMode) Option {
	return func(s *Storage) {
		s.DirMode = m
	}
}

// NewStorage creates a persistent storage backed by dir. The directory is
// created lazily by Init.
func NewStorage(dir string, options ...Option) *Storage {
	s := &Storage{
		Dir:      dir,
		FileMode: 0640,
		DirMode:  0750,
		inflight: make(map[string]struct{}),
	}
	for _, o := range options {
		o(s)
	}
	return s
}

// Init creates the directory layout and recovers queue items that were
// dequeued but never acknowledged by a previous process.
func (s *Storage) Init() error {
	if s.Dir == "" {
		return ErrInvalidStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inited {
		return nil
	}
	for _, d := range s.visitedDirs() {
		if err := os.MkdirAll(d, s.DirMode); err != nil {
			return err
		}
	}
	s.inflight = make(map[string]struct{})
	if err := s.recoverQueueLocked(); err != nil {
		return err
	}
	s.inited = true
	return nil
}

func (s *Storage) checkInit() error {
	if !s.inited || s.Dir == "" {
		return ErrInvalidStorage
	}
	return nil
}

func (s *Storage) visitedDirs() []string {
	return []string{
		filepath.Join(s.Dir, visitedDirName),
		filepath.Join(s.Dir, cookieDirName),
		filepath.Join(s.Dir, queueDirName),
		filepath.Join(s.Dir, queueDirName, readyDirName),
		filepath.Join(s.Dir, queueDirName, inflightDir),
	}
}

// urlRecord is the on-disk representation of a visited request.
type urlRecord struct {
	// RequestKey is the full request identity (method + URL + body hash).
	RequestKey string `json:"request_key"`
	// FetchedAt is the time of the last completed fetch. Zero for requests
	// which were marked visited but whose response was never recorded
	// (e.g. a process killed mid-request).
	FetchedAt time.Time `json:"fetched_at,omitempty"`
	ETag      string    `json:"etag,omitempty"`
	// LastModified mirrors the Last-Modified response header.
	LastModified string `json:"last_modified,omitempty"`
	// ContentHash is the hex encoded SHA-256 hash of the response body.
	ContentHash string `json:"content_hash,omitempty"`
}

func (s *Storage) recordPath(requestKey string) string {
	sum := sha256Hex(requestKey)
	return filepath.Join(s.Dir, visitedDirName, sum[:2], sum+".json")
}

func (s *Storage) loadRecord(requestKey string) (*urlRecord, error) {
	data, err := os.ReadFile(s.recordPath(requestKey))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec := &urlRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *Storage) storeRecord(rec *urlRecord) error {
	path := s.recordPath(rec.RequestKey)
	if err := os.MkdirAll(filepath.Dir(path), s.DirMode); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, s.FileMode)
}

// Visited implements storage.Storage. It stores the 64-bit request id in a
// legacy-compatible set; the collision-safe identity is stored separately
// through VisitedKey.
func (s *Storage) Visited(requestID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return err
	}
	return s.addLegacyIDLocked(requestID)
}

// IsVisited implements storage.Storage.
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkInit(); err != nil {
		return false, err
	}
	return s.hasLegacyIDLocked(requestID)
}

// VisitedKey implements storage.KeyedStorage. The identity is persisted
// immediately so a crash while the request is in flight does not lose it;
// however an identity whose fetch never completed is treated as unvisited
// on the next process start (see IsVisitedKey), allowing the request to be
// retried instead of blocking re-crawls forever.
func (s *Storage) VisitedKey(requestKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return err
	}
	s.inflight[requestKey] = struct{}{}
	rec, err := s.loadRecord(requestKey)
	if err != nil {
		return err
	}
	if rec != nil {
		return nil
	}
	return s.storeRecord(&urlRecord{RequestKey: requestKey})
}

// IsVisitedKey implements storage.KeyedStorage. It returns true only for
// identities which have a completed, still-fresh fetch (or, when
// RevisitInterval is zero, any completed fetch). In-flight identities of
// the current process count as visited to preserve within-process
// duplicate suppression.
func (s *Storage) IsVisitedKey(requestKey string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return false, err
	}
	if _, inflight := s.inflight[requestKey]; inflight {
		return true, nil
	}
	rec, err := s.loadRecord(requestKey)
	if err != nil {
		return false, err
	}
	// A record with a zero FetchedAt belongs to a request which was marked
	// in flight but never completed (process kill, 4xx/5xx response). Only
	// the current process still tracks it in s.inflight; after a restart it
	// must become retryable again.
	if rec == nil || rec.FetchedAt.IsZero() {
		return false, nil
	}
	if s.RevisitAfter > 0 && time.Since(rec.FetchedAt) >= s.RevisitAfter {
		return false, nil
	}
	return true, nil
}

// RevisitInterval implements storage.IncrementalStorage.
func (s *Storage) RevisitInterval() time.Duration { return s.RevisitAfter }

// GetResponseMeta implements storage.IncrementalStorage.
func (s *Storage) GetResponseMeta(requestKey string) (storage.ResponseMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkInit(); err != nil {
		return storage.ResponseMeta{}, err
	}
	rec, err := s.loadRecord(requestKey)
	if err != nil {
		return storage.ResponseMeta{}, err
	}
	if rec == nil {
		return storage.ResponseMeta{}, nil
	}
	return storage.ResponseMeta{
		FetchedAt:    rec.FetchedAt,
		ETag:         rec.ETag,
		LastModified: rec.LastModified,
		ContentHash:  rec.ContentHash,
	}, nil
}

// RecordResponse implements storage.IncrementalStorage. It is called after
// every successful fetch, including 304 revalidations and unchanged 200
// responses.
func (s *Storage) RecordResponse(requestKey string, meta storage.ResponseMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return err
	}
	delete(s.inflight, requestKey)
	rec, err := s.loadRecord(requestKey)
	if err != nil {
		return err
	}
	if rec == nil {
		rec = &urlRecord{RequestKey: requestKey}
	}
	rec.FetchedAt = meta.FetchedAt
	if meta.ETag != "" {
		rec.ETag = meta.ETag
	}
	if meta.LastModified != "" {
		rec.LastModified = meta.LastModified
	}
	rec.ContentHash = meta.ContentHash
	return s.storeRecord(rec)
}

// Close flushes pending state. All data is written synchronously, so Close
// currently only validates initialization.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inited {
		return ErrInvalidStorage
	}
	return nil
}

// Cookies implements storage.Storage. Cookies are keyed by the URL's
// scheme and host and persisted in the cookies directory.
func (s *Storage) Cookies(u *url.URL) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkInit(); err != nil {
		return ""
	}
	data, err := os.ReadFile(s.cookiePath(cookieKey(u)))
	if err != nil {
		return ""
	}
	return string(data)
}

// SetCookies implements storage.Storage.
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return
	}
	_ = writeFileAtomic(s.cookiePath(cookieKey(u)), []byte(cookies), s.FileMode)
}

func (s *Storage) cookiePath(key string) string {
	sum := sha256Hex(key)
	return filepath.Join(s.Dir, cookieDirName, sum[:2], sum+".cookie")
}

func (s *Storage) addLegacyIDLocked(requestID uint64) error {
	return s.legacyLocked().add(requestID)
}

func (s *Storage) hasLegacyIDLocked(requestID uint64) (bool, error) {
	return s.legacyLocked().has(requestID)
}

func cookieKey(u *url.URL) string {
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}
