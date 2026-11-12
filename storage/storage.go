// Copyright 2018 Adam Tauber
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Storage is an interface which handles Collector's internal data,
// like visited urls and cookies.
// The default Storage of the Collector is the InMemoryStorage.
// Collector's storage can be changed by calling Collector.SetStorage()
// function.
type Storage interface {
	// Init initializes the storage
	Init() error
	// Visited receives and stores a request ID that is visited by the Collector
	Visited(requestID uint64) error
	// IsVisited returns true if the request was visited before IsVisited
	// is called
	IsVisited(requestID uint64) (bool, error)
	// Cookies retrieves stored cookies for a given host
	Cookies(u *url.URL) string
	// SetCookies stores cookies for a given host
	SetCookies(u *url.URL, cookies string)
}

// InMemoryStorage is the default storage backend of colly.
// InMemoryStorage keeps cookies and visited urls in memory
// without persisting data on the disk.
type InMemoryStorage struct {
	visitedURLs map[uint64]bool
	lock        *sync.RWMutex
	jar         *cookiejar.Jar
}

// Init initializes InMemoryStorage
func (s *InMemoryStorage) Init() error {
	if s.visitedURLs == nil {
		s.visitedURLs = make(map[uint64]bool)
	}
	if s.lock == nil {
		s.lock = &sync.RWMutex{}
	}
	if s.jar == nil {
		var err error
		s.jar, err = cookiejar.New(nil)
		return err
	}
	return nil
}

// Visited implements Storage.Visited()
func (s *InMemoryStorage) Visited(requestID uint64) error {
	s.lock.Lock()
	s.visitedURLs[requestID] = true
	s.lock.Unlock()
	return nil
}

// IsVisited implements Storage.IsVisited()
func (s *InMemoryStorage) IsVisited(requestID uint64) (bool, error) {
	s.lock.RLock()
	visited := s.visitedURLs[requestID]
	s.lock.RUnlock()
	return visited, nil
}

// Cookies implements Storage.Cookies()
func (s *InMemoryStorage) Cookies(u *url.URL) string {
	return StringifyCookies(s.jar.Cookies(u))
}

// SetCookies implements Storage.SetCookies()
func (s *InMemoryStorage) SetCookies(u *url.URL, cookies string) {
	s.jar.SetCookies(u, UnstringifyCookies(cookies))
}

// Close implements Storage.Close()
func (s *InMemoryStorage) Close() error {
	return nil
}

// StringifyCookies serializes list of http.Cookies to string
func StringifyCookies(cookies []*http.Cookie) string {
	// Stringify cookies.
	cs := make([]string, len(cookies))
	for i, c := range cookies {
		cs[i] = c.String()
	}
	return strings.Join(cs, "\n")
}

// UnstringifyCookies deserializes a cookie string to http.Cookies
func UnstringifyCookies(s string) []*http.Cookie {
	h := http.Header{}
	for c := range strings.SplitSeq(s, "\n") {
		h.Add("Set-Cookie", c)
	}
	r := http.Response{Header: h}
	return r.Cookies()
}

// ContainsCookie checks if a cookie name is represented in cookies
func ContainsCookie(cookies []*http.Cookie, name string) bool {
	for _, c := range cookies {
		if c.Name == name {
			return true
		}
	}
	return false
}

// KeyedStorage is an optional extension of Storage. Backends implementing it
// identify requests by their full identity (method, normalized URL and body)
// instead of the 64-bit FNV-1a hash used by Visited/IsVisited. Because the
// identity string is not truncated to 64 bits, two distinct requests whose
// FNV hashes collide cannot make the Collector skip an unvisited page.
//
// The Collector calls these methods when the configured Storage implements
// them and falls back to Visited/IsVisited otherwise, so existing Storage
// implementations keep working unchanged.
type KeyedStorage interface {
	// VisitedKey stores a request identity while the request is in flight.
	VisitedKey(requestKey string) error
	// IsVisitedKey reports whether the request identity is already known.
	IsVisitedKey(requestKey string) (bool, error)
}

// ResponseMeta stores the freshness metadata of a previously fetched
// response. It is used by IncrementalStorage backends to decide whether a
// re-crawl is due and to send conditional requests.
type ResponseMeta struct {
	// FetchedAt is the time of the last successful fetch (content changed
	// or revalidated as unchanged).
	FetchedAt time.Time
	// ETag is the entity tag returned by the server (ETag header).
	ETag string
	// LastModified is the value of the Last-Modified response header.
	LastModified string
	// ContentHash is the hex encoded SHA-256 hash of the response body.
	ContentHash string
}

// IncrementalStorage is an optional extension of Storage implemented by
// backends which support incremental re-crawling. When the Collector's
// Storage implements it:
//
//   - a URL fetched less than the backend's configured expiration duration
//     ago is skipped without any HTTP request;
//   - once expired, the URL is re-fetched and the stored ETag /
//     Last-Modified are sent as If-None-Match / If-Modified-Since headers;
//   - a 304 response, or a 200 response whose body hash equals the stored
//     one, only refreshes the stored metadata: OnResponse, OnHTML, OnXML and
//     OnScraped callbacks are not invoked;
//   - a 200 response with a changed body is processed normally and the
//     stored metadata is replaced.
//
// All methods must be safe for concurrent use.
type IncrementalStorage interface {
	// RevisitInterval returns the duration after which a stored response is
	// considered stale. A zero or negative duration disables incremental
	// re-crawling and the backend behaves like a plain visited-set storage.
	RevisitInterval() time.Duration
	// GetResponseMeta returns the stored metadata for a request identity
	// built with RequestKey. A zero ResponseMeta is returned with a nil error
	// when the identity is not stored.
	GetResponseMeta(requestKey string) (ResponseMeta, error)
	// RecordResponse persists the metadata of a completed fetch. It is called
	// after every successful (2xx) response, including unchanged ones.
	RecordResponse(requestKey string, meta ResponseMeta) error
}

// RequestKey builds the full identity string of a request. The key includes
// the HTTP method, the normalized URL and the exact request body, so POST
// requests with different bodies and requests whose 64-bit FNV hashes
// collide remain distinguishable.
//
// normalizedURL must be the request URL normalized the same way the
// Collector normalizes URLs internally; body may be nil.
func RequestKey(method, normalizedURL string, body []byte) string {
	var b strings.Builder
	b.Grow(len(method) + 1 + len(normalizedURL) + 1 + len(body))
	b.WriteString(method)
	b.WriteByte(' ')
	b.WriteString(normalizedURL)
	if len(body) > 0 {
		b.WriteByte(' ')
		sum := sha256.Sum256(body)
		b.WriteString(hex.EncodeToString(sum[:]))
	}
	return b.String()
}
