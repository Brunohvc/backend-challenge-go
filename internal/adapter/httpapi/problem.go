package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

const (
	codeInvalidRequest         = "INVALID_REQUEST"
	codeUnsupportedMediaType   = "UNSUPPORTED_MEDIA_TYPE"
	codePayloadTooLarge        = "PAYLOAD_TOO_LARGE"
	codeUnauthenticated        = "UNAUTHENTICATED"
	codeForbidden              = "FORBIDDEN"
	codeNotFound               = "NOT_FOUND"
	codeMethodNotAllowed       = "METHOD_NOT_ALLOWED"
	codeTemporarilyUnavailable = "TEMPORARILY_UNAVAILABLE"
	codeInternalError          = "INTERNAL_ERROR"
)

const retryAfterSeconds = "2"

type problem struct {
	Type          string         `json:"type"`
	Title         string         `json:"title"`
	Status        int            `json:"status"`
	Code          string         `json:"code"`
	Detail        string         `json:"detail,omitempty"`
	CorrelationID string         `json:"correlationId,omitempty"`
	Errors        []fieldProblem `json:"errors,omitempty"`
}

type problemBody interface {
	status() int
}

func (p problem) status() int { return p.Status }

type fieldProblem struct {
	Field string `json:"field"`
	Code  string `json:"code"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string, fields []wager.FieldError) {
	sendProblem(w, newProblem(w, r, status, code, title, detail, fields))
}

func newProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string, fields []wager.FieldError) problem {
	p := problem{
		Type:          "about:blank",
		Title:         title,
		Status:        status,
		Code:          code,
		Detail:        detail,
		CorrelationID: correlationID(w, r),
	}
	for _, f := range fields {
		p.Errors = append(p.Errors, fieldProblem(f))
	}
	return p
}

func sendProblem(w http.ResponseWriter, p problemBody) {
	if p.status() == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", retryAfterSeconds)
	}
	writeBody(w, p.status(), "application/problem+json", p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	writeBody(w, status, "application/json", v)
}

func writeBody(w http.ResponseWriter, status int, contentType string, v any) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func correlationID(w http.ResponseWriter, r *http.Request) string {
	if id := logging.FieldsFrom(r.Context()).CorrelationID; id != "" {
		return id
	}
	return w.Header().Get(headerCorrelationID)
}
