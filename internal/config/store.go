package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Source provides the rules in force right now. *Rules and *Store both
// implement it.
type Source interface {
	Current() *Rules
}

// ReloadResult says what a reload did.
type ReloadResult string

const (
	// Reloaded means new rules were parsed and swapped in.
	Reloaded ReloadResult = "reloaded"
	// Unchanged means the file content was identical, so nothing changed.
	Unchanged ReloadResult = "unchanged"
	// Rejected means the file could not be read or parsed; the previous
	// rules stay in force.
	Rejected ReloadResult = "rejected"
)

// Store holds the rules loaded from a file and swaps them atomically on
// reload. Requests in flight keep the *Rules they started with, and a bad
// file never replaces good rules.
type Store struct {
	path string
	snap atomic.Pointer[Snapshot]

	mu sync.Mutex // serialises reloads

	// OnReload, if set, is called after every reload attempt.
	OnReload func(ReloadResult, error)
}

// NewStore loads path and returns a Store serving it. It fails if the
// initial file is missing or invalid.
func NewStore(path string) (*Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	rs, err := ParseRules(data)
	if err != nil {
		return nil, err
	}
	s := &Store{path: path}
	s.snap.Store(newSnapshot(rs, data))
	return s, nil
}

// Snapshot is the rule set in force together with where it came from. The
// rules and their metadata are swapped as one value, so readers never see
// new rules with an old hash.
type Snapshot struct {
	Rules *Rules
	// SHA256 is the hex digest of the file content the rules were parsed from.
	SHA256   string
	LoadedAt time.Time
}

func newSnapshot(rs *Rules, data []byte) *Snapshot {
	h := sha256.Sum256(data)
	return &Snapshot{Rules: rs, SHA256: hex.EncodeToString(h[:]), LoadedAt: time.Now().UTC()}
}

// Current implements Source.
func (s *Store) Current() *Rules { return s.snap.Load().Rules }

// Snapshot returns the rules in force and their file hash and load time.
func (s *Store) Snapshot() *Snapshot { return s.snap.Load() }

// Path is the file the store reads.
func (s *Store) Path() string { return s.path }

// Reload re-reads the file. Invalid content is rejected and the current
// rules are kept.
func (s *Store) Reload() (ReloadResult, error) {
	res, err := s.reload()
	if s.OnReload != nil {
		s.OnReload(res, err)
	}
	return res, err
}

func (s *Store) reload() (ReloadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		return Rejected, fmt.Errorf("config: %w", err)
	}
	h := sha256.Sum256(data)
	if hex.EncodeToString(h[:]) == s.snap.Load().SHA256 {
		return Unchanged, nil
	}
	rs, err := ParseRules(data)
	if err != nil {
		return Rejected, err
	}
	s.snap.Store(newSnapshot(rs, data))
	return Reloaded, nil
}

// Watch reloads whenever trigger fires (for example on SIGHUP) and, if
// interval > 0, also polls the file on that interval. Polling by content
// hash works with editors that replace files and with Kubernetes ConfigMap
// symlink swaps, where inotify-style watches are unreliable. Watch returns
// when ctx is done.
func (s *Store) Watch(ctx context.Context, interval time.Duration, trigger <-chan os.Signal) {
	var tick <-chan time.Time
	if interval > 0 {
		t := time.NewTicker(interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-trigger:
		case <-tick:
		}
		_, _ = s.Reload()
	}
}
