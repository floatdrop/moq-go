package main

import (
	"net/http"
	"net/http/pprof"
)

// pprofHandler serves net/http/pprof under /debug/pprof/ on a mux of its own:
// importing the package registers the same handlers on http.DefaultServeMux,
// which nothing here serves, so they are reachable only on -pprof-addr.
func pprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
