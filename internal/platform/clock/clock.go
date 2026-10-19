// Package clock is the only source of wall-clock time for persisted data and decisions.
package clock

import "time"

// System reads the real clock and always reports UTC.
type System struct{}

// Now returns the current time in UTC.
func (System) Now() time.Time {
	return time.Now().UTC()
}
