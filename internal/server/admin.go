package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/mananbatraa/RateLimiter/internal/config"
)

// snapshotter is implemented by *config.Store. A fixed *config.Rules has no
// file behind it, so the admin view omits the hash for it.
type snapshotter interface {
	Snapshot() *config.Snapshot
}

type ruleView struct {
	Name       string   `json:"name,omitempty"`
	PathPrefix string   `json:"path_prefix,omitempty"`
	Methods    []string `json:"methods,omitempty"`
	Algorithm  string   `json:"algorithm"`
	Limit      int64    `json:"limit"`
	Window     string   `json:"window"`
	Burst      int64    `json:"burst,omitempty"`
	Key        string   `json:"key"`
	Cost       int64    `json:"cost"`
	Shadow     bool     `json:"shadow,omitempty"`
}

type rulesView struct {
	SHA256        string     `json:"sha256,omitempty"`
	LoadedAt      *time.Time `json:"loaded_at,omitempty"`
	FailurePolicy string     `json:"failure_policy"`
	LocalReplicas int64      `json:"local_replicas"`
	Rules         []ruleView `json:"rules"`
	Default       *ruleView  `json:"default,omitempty"`
}

func viewRule(r config.Rule) ruleView {
	return ruleView{
		Name: r.Name, PathPrefix: r.PathPrefix, Methods: r.Methods,
		Algorithm: string(r.Algorithm), Limit: r.Limit, Window: r.Window.String(),
		Burst: r.Burst, Key: r.Key, Cost: r.Cost, Shadow: r.Shadow,
	}
}

func viewRules(src config.Source) rulesView {
	var v rulesView
	rs := src.Current()
	if s, ok := src.(snapshotter); ok {
		// Take rules from the snapshot so the hash always matches them.
		snap := s.Snapshot()
		rs = snap.Rules
		v.SHA256, v.LoadedAt = snap.SHA256, &snap.LoadedAt
	}
	v.FailurePolicy, v.LocalReplicas = string(rs.FailurePolicy), rs.LocalReplicas
	v.Rules = make([]ruleView, 0, len(rs.Rules))
	for _, r := range rs.Rules {
		v.Rules = append(v.Rules, viewRule(r))
	}
	if rs.Default != nil {
		d := viewRule(*rs.Default)
		v.Default = &d
	}
	return v
}

// adminRules serves the active rule set as JSON to holders of the admin
// token. With no token configured the endpoint does not exist (404), so a
// default deployment exposes nothing new.
func adminRules(token string, src config.Source) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
			return
		}
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ratelimiter-admin"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, viewRules(src))
	}
}
