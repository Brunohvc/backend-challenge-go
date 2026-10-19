package event

import (
	"errors"
	"time"
)

const timestampLayout = "2006-01-02T15:04:05.000Z"

// ErrInvalidTimestamp marks a Timestamp that cannot be written on the wire.
var ErrInvalidTimestamp = errors.New("event: timestamp cannot be represented")

// Timestamp is a time that marshals as UTC with millisecond precision.
type Timestamp time.Time

// MarshalJSON encodes t as a UTC string with millisecond precision.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	at := time.Time(t).UTC()
	if at.IsZero() {
		return nil, ErrInvalidTimestamp
	}
	if year := at.Year(); year < 0 || year > 9999 {
		return nil, ErrInvalidTimestamp
	}

	buf := make([]byte, 0, len(timestampLayout)+2)
	buf = append(buf, '"')
	buf = at.AppendFormat(buf, timestampLayout)
	buf = append(buf, '"')
	return buf, nil
}
