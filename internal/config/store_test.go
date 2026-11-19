package config

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const rulesV1 = "default: {limit: 1, window: 1s}\n"
const rulesV2 = "failure_policy: closed\ndefault: {limit: 2, window: 1s}\n"

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.yaml")
	writeFile(t, path, rulesV1)
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestStoreReload(t *testing.T) {
	s, path := newTestStore(t)
	if s.Path() != path || s.Current().Default.Limit != 1 {
		t.Fatalf("initial store wrong: %+v", s.Current())
	}

	var results []ReloadResult
	s.OnReload = func(r ReloadResult, _ error) { results = append(results, r) }

	if res, err := s.Reload(); res != Unchanged || err != nil {
		t.Fatalf("reload of same content = %s, %v", res, err)
	}

	writeFile(t, path, rulesV2)
	before := s.Current()
	if res, err := s.Reload(); res != Reloaded || err != nil {
		t.Fatalf("reload = %s, %v", res, err)
	}
	if got := s.Current(); got.Default.Limit != 2 || got.FailurePolicy != FailClosed {
		t.Fatalf("rules not swapped: %+v", got)
	}
	if before.Default.Limit != 1 {
		t.Fatal("rules held by in-flight requests must not be mutated")
	}

	// A broken file is rejected and the last good rules stay in force.
	writeFile(t, path, "rules: [{name: x, limit: -1}]")
	if res, err := s.Reload(); res != Rejected || err == nil {
		t.Fatalf("reload of invalid file = %s, %v", res, err)
	}
	if s.Current().Default.Limit != 2 {
		t.Fatal("invalid file replaced good rules")
	}
	// Fixing the file is picked up even though the last attempt failed.
	writeFile(t, path, rulesV1)
	if res, _ := s.Reload(); res != Reloaded {
		t.Fatalf("reload after fix = %s", res)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if res, err := s.Reload(); res != Rejected || err == nil {
		t.Fatalf("reload of missing file = %s, %v", res, err)
	}
	want := []ReloadResult{Unchanged, Reloaded, Rejected, Reloaded, Rejected}
	if len(results) != len(want) {
		t.Fatalf("OnReload saw %v, want %v", results, want)
	}
	for i := range want {
		if results[i] != want[i] {
			t.Fatalf("OnReload saw %v, want %v", results, want)
		}
	}
}

func TestNewStoreErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewStore(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("missing file should fail")
	}
	bad := filepath.Join(dir, "bad.yaml")
	writeFile(t, bad, "failure_policy: sometimes")
	if _, err := NewStore(bad); err == nil {
		t.Fatal("invalid file should fail")
	}
}

func TestStoreWatch(t *testing.T) {
	s, path := newTestStore(t)
	reloaded := make(chan ReloadResult, 16)
	s.OnReload = func(r ReloadResult, _ error) { reloaded <- r }

	ctx, cancel := context.WithCancel(context.Background())
	trigger := make(chan os.Signal, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Watch(ctx, 10*time.Millisecond, trigger)
	}()

	// Polling picks the change up on its own.
	writeFile(t, path, rulesV2)
	waitFor(t, reloaded, Reloaded)
	if s.Current().Default.Limit != 2 {
		t.Fatal("poll did not apply new rules")
	}

	// A signal triggers a reload too.
	trigger <- os.Interrupt
	waitFor(t, reloaded, Unchanged)

	cancel()
	wg.Wait() // Watch returns once the context is done
}

func TestStoreWatchSignalOnly(t *testing.T) {
	s, path := newTestStore(t)
	reloaded := make(chan ReloadResult, 4)
	s.OnReload = func(r ReloadResult, _ error) { reloaded <- r }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trigger := make(chan os.Signal, 1)
	go s.Watch(ctx, 0, trigger)

	writeFile(t, path, rulesV2)
	select {
	case r := <-reloaded:
		t.Fatalf("reloaded (%s) without a signal while polling is off", r)
	case <-time.After(50 * time.Millisecond):
	}
	trigger <- os.Interrupt
	waitFor(t, reloaded, Reloaded)
}

func waitFor(t *testing.T, ch <-chan ReloadResult, want ReloadResult) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case got := <-ch:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

func TestStoreSnapshotTracksFileHash(t *testing.T) {
	s, path := newTestStore(t)
	first := s.Snapshot()
	if len(first.SHA256) != 64 || first.LoadedAt.IsZero() || first.Rules != s.Current() {
		t.Fatalf("bad initial snapshot: %+v", first)
	}

	writeFile(t, path, "not: [valid")
	if res, _ := s.Reload(); res != Rejected || s.Snapshot() != first {
		t.Fatalf("rejected reload must keep the snapshot, got %v", res)
	}

	writeFile(t, path, rulesV2)
	if res, err := s.Reload(); res != Reloaded || err != nil {
		t.Fatalf("reload: %v %v", res, err)
	}
	second := s.Snapshot()
	if second.SHA256 == first.SHA256 || second.Rules.Default.Limit != 2 || second.Rules != s.Current() {
		t.Fatalf("snapshot not swapped: %+v", second)
	}

	writeFile(t, path, rulesV1)
	if res, _ := s.Reload(); res != Reloaded || s.Snapshot().SHA256 != first.SHA256 {
		t.Fatal("same content must hash the same")
	}
}
