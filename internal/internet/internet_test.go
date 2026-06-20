package internet_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aurora-capcompute/internal/internet"
)

func TestAllowedGETSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	policy := mustPolicy(t, "GET:"+server.URL)
	client := internet.NewClient(policy)

	response, err := client.Read(context.Background(), internet.ReadRequest{Method: "GET", URL: server.URL})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if response.Status != http.StatusOK || response.Body != "ok" {
		t.Fatalf("response = %+v", response)
	}
}

func TestDisallowedDomainFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("request should not reach disallowed server")
	}))
	defer server.Close()

	client := internet.NewClient(mustPolicy(t, "GET:https://example.com"))

	_, err := client.Read(context.Background(), internet.ReadRequest{Method: "GET", URL: server.URL})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not allowlisted") {
		t.Fatalf("error = %v", err)
	}
}

func TestDisallowedMethodFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("request should not reach server")
	}))
	defer server.Close()

	client := internet.NewClient(mustPolicy(t, "GET:"+server.URL))

	_, err := client.Read(context.Background(), internet.ReadRequest{Method: "POST", URL: server.URL})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "GET only") {
		t.Fatalf("error = %v", err)
	}
}

func TestWildcardAllowsAnyHTTPOrigin(t *testing.T) {
	policy := mustPolicy(t, "GET:*")
	for _, target := range []string{"https://example.com/path", "http://localhost:8080/value"} {
		if err := policy.Allows(http.MethodGet, target); err != nil {
			t.Fatalf("allow %s: %v", target, err)
		}
	}
	if err := policy.Allows(http.MethodPost, "https://example.com"); err == nil {
		t.Fatal("wildcard unexpectedly allowed POST")
	}
}

func TestNonHTTPSchemeFails(t *testing.T) {
	client := internet.NewClient(mustPolicy(t, "GET:https://example.com"))

	_, err := client.Read(context.Background(), internet.ReadRequest{Method: "GET", URL: "file:///tmp/data.txt"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("error = %v", err)
	}
}

func TestRedirectToDisallowedDomainFails(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("redirect target should not be reached")
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	client := internet.NewClient(mustPolicy(t, "GET:"+source.URL))

	_, err := client.Read(context.Background(), internet.ReadRequest{Method: "GET", URL: source.URL})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not allowlisted") {
		t.Fatalf("error = %v", err)
	}
}

func TestResponseBodyIsByteLimited(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("abcdef"))
	}))
	defer server.Close()

	client := internet.NewClient(mustPolicy(t, "GET:"+server.URL))
	client.MaxBytes = 3

	response, err := client.Read(context.Background(), internet.ReadRequest{Method: "GET", URL: server.URL})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if response.Body != "abc" {
		t.Fatalf("body = %q, want abc", response.Body)
	}
}

func mustPolicy(t *testing.T, raw string) internet.Policy {
	t.Helper()
	policy, err := internet.ParseAllowlist(raw)
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	return policy
}
