package persistent

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// The legacy visited-set keeps the 64-bit request ids computed by colly
// (storage.Storage.Visited/IsVisited). These ids are kept in an append-only
// log plus an in-memory set. The log is rewritten (compacted) when it grows
// beyond legacyCompactThreshold entries.
//
// Distinct request identities are stored without truncation through
// VisitedKey (see storage.go), so an FNV hash collision between two
// different requests cannot hide an unvisited page: the legacy set is only
// consulted by backends/collectors that do not implement KeyedStorage.
const (
	legacyLogName          = "legacy_ids.log"
	legacyCompactThreshold = 4096
)

type legacyStore struct {
	mu     sync.Mutex
	path   string
	ids    map[uint64]struct{}
	loaded bool
}

func (s *Storage) legacyPath() string {
	return filepath.Join(s.Dir, visitedDirName, legacyLogName)
}

func (s *Storage) legacyLocked() *legacyStore {
	if s.legacy == nil {
		s.legacy = &legacyStore{path: s.legacyPath(), ids: make(map[uint64]struct{})}
	}
	return s.legacy
}

func (l *legacyStore) load() error {
	if l.loaded {
		return nil
	}
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			l.loaded = true
			return nil
		}
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64), 1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		id, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			continue
		}
		l.ids[id] = struct{}{}
	}
	l.loaded = true
	return scanner.Err()
}

func (l *legacyStore) add(id uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.ids[id]; ok {
		return nil
	}
	if err := l.load(); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(id, 10) + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	l.ids[id] = struct{}{}
	if len(l.ids) > legacyCompactThreshold {
		return l.compact()
	}
	return nil
}

func (l *legacyStore) has(id uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.load(); err != nil {
		return false, err
	}
	_, ok := l.ids[id]
	return ok, nil
}

// compact rewrites the log containing only the current id set.
func (l *legacyStore) compact() error {
	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for id := range l.ids {
		if _, err := w.WriteString(strconv.FormatUint(id, 10) + "\n"); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, l.path)
}
