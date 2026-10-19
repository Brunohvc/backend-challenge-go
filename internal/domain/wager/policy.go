package wager

import "time"

// ReferencePolicy says how long a transaction waits for the one it refers to.
type ReferencePolicy struct {
	MaxAttempts int
	TTL         time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Jitter      func(time.Duration) time.Duration
}

// NextAttemptAt returns when to look for the reference again after attempt misses.
func (p ReferencePolicy) NextAttemptAt(attempt int, now time.Time) time.Time {
	delay := p.backoff(max(attempt, 0))
	if p.Jitter != nil {
		delay = p.Jitter(delay)
	}
	return now.Add(delay)
}

func (p ReferencePolicy) backoff(attempt int) time.Duration {
	if p.BackoffBase > p.BackoffMax>>attempt {
		return p.BackoffMax
	}
	return p.BackoffBase << attempt
}

func (p ReferencePolicy) validate() error {
	switch {
	case p.MaxAttempts < 1:
		return invalidDecision("the reference policy needs at least one attempt")
	case p.TTL <= 0 || p.BackoffBase <= 0 || p.BackoffMax <= 0:
		return invalidDecision("the TTL and the backoff of the reference policy must be positive")
	case p.BackoffBase > p.BackoffMax:
		return invalidDecision("the backoff base of the reference policy must not exceed its max")
	}
	return nil
}
