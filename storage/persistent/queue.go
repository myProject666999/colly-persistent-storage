package persistent

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrQueueEmpty is returned by GetRequest when no pending request exists.
var ErrQueueEmpty = errors.New("persistent queue is empty")

// Queue methods are provided on the same Storage value so a single
// directory can back both the Collector storage and the request queue.
//
// Requests live in two directories:
//
//   - queue/ready:   requests waiting to be dequeued;
//   - queue/inflight: requests dequeued with GetRequestForAck but not yet
//     acknowledged with AckRequest.
//
// On Init every file left in inflight by a previous process (i.e. requests
// that were being processed when the process was killed, even with
// kill -9) is moved back to ready, so no request is lost. A request is
// removed from inflight only after the caller acknowledges its completion,
// hence successfully processed requests are never executed twice because of
// a crash.

func (s *Storage) readyDir() string {
	return filepath.Join(s.Dir, queueDirName, readyDirName)
}

func (s *Storage) inflightDirPath() string {
	return filepath.Join(s.Dir, queueDirName, inflightDir)
}

// queueSeq hands out monotonically increasing, process-unique sequence
// numbers used as file names. The counter starts above every sequence
// number found on disk, so a recovered item can never be overwritten.
type queueSeq struct {
	mu sync.Mutex
	n  uint64
}

func (s *Storage) nextSeqLocked() (uint64, error) {
	if s.seq == nil {
		s.seq = &queueSeq{}
		n, err := s.scanMaxSeqLocked()
		if err != nil {
			return 0, err
		}
		s.seq.n = n
	}
	s.seq.mu.Lock()
	s.seq.n++
	n := s.seq.n
	s.seq.mu.Unlock()
	return n, nil
}

func (s *Storage) scanMaxSeqLocked() (uint64, error) {
	var max uint64
	for _, dir := range []string{s.readyDir(), s.inflightDirPath()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		for _, e := range entries {
			if n, ok := parseSeqName(e.Name()); ok && n > max {
				max = n
			}
		}
	}
	return max, nil
}

func parseSeqName(name string) (uint64, bool) {
	name = strings.TrimSuffix(name, ".req")
	n, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func seqName(n uint64) string {
	// Zero padded names keep lexical order identical to numeric order.
	return strconv.FormatUint(n, 10) + ".req"
}

// recoverQueueLocked moves every unacknowledged request of a previous
// process back to the ready directory.
func (s *Storage) recoverQueueLocked() error {
	entries, err := os.ReadDir(s.inflightDirPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		from := filepath.Join(s.inflightDirPath(), e.Name())
		to := filepath.Join(s.readyDir(), e.Name())
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	return nil
}

// AddRequest implements queue.Storage. The serialized request is fsynced to
// the ready directory before the method returns, so it survives an
// immediate process kill.
func (s *Storage) AddRequest(r []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return err
	}
	n, err := s.nextSeqLocked()
	if err != nil {
		return err
	}
	path := filepath.Join(s.readyDir(), seqName(n))
	return writeFileAtomic(path, r, s.FileMode)
}

// GetRequest implements queue.Storage. It dequeues the oldest ready request
// and acknowledges it immediately, matching the non-acknowledging
// queue.Storage contract. Users needing crash-safe processing must use
// GetRequestForAck together with AckRequest.
func (s *Storage) GetRequest() ([]byte, error) {
	data, id, err := s.GetRequestForAck()
	if err != nil {
		return nil, err
	}
	if err := s.AckRequest(id); err != nil {
		return nil, err
	}
	return data, nil
}

// GetRequestForAck implements queue.AckableStorage. It atomically moves the
// oldest ready request into the inflight directory and returns the request
// payload together with an opaque ack id. The request stays on disk until
// AckRequest is called; if the process dies before that, Init moves it back
// to ready.
func (s *Storage) GetRequestForAck() (data []byte, ackID uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return nil, 0, err
	}
	name, err := s.oldestReadyLocked()
	if err != nil {
		return nil, 0, err
	}
	from := filepath.Join(s.readyDir(), name)
	to := filepath.Join(s.inflightDirPath(), name)
	if err := os.Rename(from, to); err != nil {
		return nil, 0, err
	}
	id, ok := parseSeqName(name)
	if !ok {
		return nil, 0, ErrQueueEmpty
	}
	data, err = os.ReadFile(to)
	if err != nil {
		return nil, 0, err
	}
	return data, id, nil
}

// AckRequest removes a request previously returned by GetRequestForAck.
// Unknown ids are ignored: the request may already have been acknowledged
// by another consumer.
func (s *Storage) AckRequest(ackID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkInit(); err != nil {
		return err
	}
	return removeFile(filepath.Join(s.inflightDirPath(), seqName(ackID)))
}

func (s *Storage) oldestReadyLocked() (string, error) {
	entries, err := os.ReadDir(s.readyDir())
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return "", ErrQueueEmpty
	}
	sort.Strings(names)
	return names[0], nil
}

// QueueSize implements queue.Storage. Only ready (not inflight) requests
// are counted, matching the semantics of colly's in-memory queue storage.
func (s *Storage) QueueSize() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkInit(); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(s.readyDir())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count, nil
}
