package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

type decodeError struct {
	Status int
	Code   string
	Field  string
	Detail string
	err    error
}

func (e *decodeError) Error() string {
	if e.err != nil {
		return "httpapi: decode body: " + e.Detail + ": " + e.err.Error()
	}
	return "httpapi: decode body: " + e.Detail
}

func (e *decodeError) Unwrap() error { return e.err }

func invalidBody(detail, field string, err error) *decodeError {
	return &decodeError{Status: http.StatusBadRequest, Code: codeInvalidRequest, Field: field, Detail: detail, err: err}
}

func decodeJSON(r *http.Request, maxBytes int64, dst any) error {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return &decodeError{Status: http.StatusUnsupportedMediaType, Code: codeUnsupportedMediaType,
			Detail: "Content-Type must be application/json", err: err}
	}

	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBytes))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return &decodeError{Status: http.StatusRequestEntityTooLarge, Code: codePayloadTooLarge,
			Detail: fmt.Sprintf("body exceeds %d bytes", maxBytes), err: err}
	}
	if err != nil {
		return invalidBody("body could not be read", "", err)
	}
	if trimmed := bytes.TrimSpace(body); len(trimmed) == 0 || trimmed[0] != '{' {
		return invalidBody("body must be a JSON object", "", nil)
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return classify(err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return invalidBody("body must hold a single JSON object", "", err)
	}
	return nil
}

func classify(err error) *decodeError {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return invalidBody(fmt.Sprintf("field %s has the wrong type", typeErr.Field), typeErr.Field, err)
	}
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		field := strings.Trim(name, `"`)
		return invalidBody(fmt.Sprintf("unknown field %s", field), field, err)
	}
	return invalidBody("body is not valid JSON", "", err)
}

func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var de *decodeError
	if !errors.As(err, &de) {
		writeProblem(w, r, http.StatusInternalServerError, codeInternalError, "Internal error", "", nil)
		return
	}
	writeProblem(w, r, de.Status, de.Code, http.StatusText(de.Status), de.Detail, nil)
}
