package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

const cursorVersion = 1

var errInvalidCursor = errors.New("httpapi: invalid cursor")

type cursorBody struct {
	V   int   `json:"v"`
	Seq int64 `json:"seq"`
}

func encodeCursor(seq int64) string {
	b, err := json.Marshal(cursorBody{V: cursorVersion, Seq: seq})
	if err != nil {
		panic(fmt.Errorf("httpapi: encode cursor: %w", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return 0, errInvalidCursor
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c cursorBody
	if err := dec.Decode(&c); err != nil || dec.More() {
		return 0, errInvalidCursor
	}
	if c.V != cursorVersion || c.Seq < 0 || encodeCursor(c.Seq) != s {
		return 0, errInvalidCursor
	}
	return c.Seq, nil
}
