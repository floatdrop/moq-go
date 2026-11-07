package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The profiles cmd/confbench fetches are served, and nothing outside
// /debug/pprof/ is.
func TestPprofHandler(t *testing.T) {
	t.Parallel()
	h := pprofHandler()
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/debug/pprof/", http.StatusOK, "goroutine"},
		{"/debug/pprof/goroutine?debug=1", http.StatusOK, "goroutine profile:"},
		{"/debug/pprof/profile?seconds=1", http.StatusOK, ""},
		{"/healthz", http.StatusNotFound, ""},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.code || !strings.Contains(rec.Body.String(), tc.body) {
			t.Errorf("GET %s = %d, body has %q: %v; want %d", tc.path, rec.Code, tc.body,
				strings.Contains(rec.Body.String(), tc.body), tc.code)
		}
	}
}
