package poller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-github/v66/github"
)

func newTestAdapter(t *testing.T, handler http.HandlerFunc) (*GHClientAdapter, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	client := github.NewClient(srv.Client())
	baseURL := srv.URL + "/"
	u, err := client.BaseURL.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse base url: %v", err)
	}
	client.BaseURL = u
	return &GHClientAdapter{Client: client}, srv
}

func TestGHClientAdapter_ListRepos_Paginates(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/orgs/acme/repos") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "", "1":
			w.Header().Set("Link", fmt.Sprintf(`<%s/orgs/acme/repos?page=2>; rel="next"`, srv2URL(r)))
			_ = json.NewEncoder(w).Encode([]map[string]any{{"name": "widgets"}})
		default:
			_ = json.NewEncoder(w).Encode([]map[string]any{{"name": "gadgets"}})
		}
	})
	defer srv.Close()

	names, err := adapter.ListRepos(t.Context(), "acme")
	if err != nil {
		t.Fatalf("ListRepos: %v", err)
	}
	if len(names) != 2 || names[0] != "widgets" || names[1] != "gadgets" {
		t.Fatalf("unexpected repo list: %+v", names)
	}
}

// srv2URL reconstructs "scheme://host" from an inbound request so the Link
// header's next-page URL points back at the same test server.
func srv2URL(r *http.Request) string {
	scheme := "http"
	return scheme + "://" + r.Host
}

func TestGHClientAdapter_ListRepos_Error(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	if _, err := adapter.ListRepos(t.Context(), "acme"); err == nil {
		t.Fatal("expected error from ListRepos on server 500")
	}
}

func TestGHClientAdapter_ListActiveWorkflowRuns(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/actions/runs") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		status := r.URL.Query().Get("status")
		w.Header().Set("Content-Type", "application/json")
		var runs []map[string]any
		if status == "queued" {
			runs = []map[string]any{{"id": 1, "status": "queued"}}
		} else {
			runs = []map[string]any{{"id": 2, "status": "in_progress"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(runs), "workflow_runs": runs})
	})
	defer srv.Close()

	runs, err := adapter.ListActiveWorkflowRuns(t.Context(), "acme", "widgets")
	if err != nil {
		t.Fatalf("ListActiveWorkflowRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs (queued+in_progress), got %d", len(runs))
	}
}

func TestGHClientAdapter_ListActiveWorkflowRuns_Error(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	if _, err := adapter.ListActiveWorkflowRuns(t.Context(), "acme", "widgets"); err == nil {
		t.Fatal("expected error from ListActiveWorkflowRuns on server 500")
	}
}

func TestGHClientAdapter_ListRunnerGroups(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/actions/runner-groups") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count":   1,
			"runner_groups": []map[string]any{{"id": 1, "name": "default"}},
		})
	})
	defer srv.Close()

	groups, err := adapter.ListRunnerGroups(t.Context(), "acme")
	if err != nil {
		t.Fatalf("ListRunnerGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].GetName() != "default" {
		t.Fatalf("unexpected groups: %+v", groups)
	}
}

func TestGHClientAdapter_ListRunnerGroups_Error(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	if _, err := adapter.ListRunnerGroups(t.Context(), "acme"); err == nil {
		t.Fatal("expected error from ListRunnerGroups on server 500")
	}
}

func TestGHClientAdapter_ListRunnerGroupRunners(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/actions/runner-groups/1/runners") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"runners":     []map[string]any{{"id": 1, "name": "runner-1", "busy": true}},
		})
	})
	defer srv.Close()

	runners, err := adapter.ListRunnerGroupRunners(t.Context(), "acme", 1)
	if err != nil {
		t.Fatalf("ListRunnerGroupRunners: %v", err)
	}
	if len(runners) != 1 || !runners[0].GetBusy() {
		t.Fatalf("unexpected runners: %+v", runners)
	}
}

func TestGHClientAdapter_ListRunnerGroupRunners_Error(t *testing.T) {
	adapter, srv := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	if _, err := adapter.ListRunnerGroupRunners(t.Context(), "acme", 1); err == nil {
		t.Fatal("expected error from ListRunnerGroupRunners on server 500")
	}
}
