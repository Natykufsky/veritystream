package veritystream

import (
	_ "embed"
	"net/http"
)

//go:embed web/console.html
var defaultConsoleHTML []byte

// ConsoleHandler returns an http.Handler serving the production-grade VerityStream
// interactive web console for testing real-time chat, AI token streams, and chunked file uploads.
func ConsoleHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(defaultConsoleHTML)
	})
}

