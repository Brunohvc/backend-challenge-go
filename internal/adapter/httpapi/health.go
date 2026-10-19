package httpapi

import (
	"log/slog"
	"net/http"
)

const checkFailed = "fail"

type healthBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (h *handlers) registerHealth(mux routeMux) {
	mux.HandleFunc("GET /health/live", h.live)
	mux.HandleFunc("GET /health/ready", h.ready)
}

func (h *handlers) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthBody{Status: "ok"})
}

func (h *handlers) ready(w http.ResponseWriter, r *http.Request) {
	ok, results := h.deps.Health.Ready(r.Context())
	checks := make(map[string]string, len(results))
	for name, result := range results {
		if result == "ok" {
			checks[name] = "ok"
			continue
		}
		checks[name] = checkFailed
		h.deps.Log.WarnContext(r.Context(), "readiness check failed",
			slog.String("check", name), slog.String("error", result))
	}
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, healthBody{Status: "unavailable", Checks: checks})
		return
	}
	writeJSON(w, http.StatusOK, healthBody{Status: "ok", Checks: checks})
}
