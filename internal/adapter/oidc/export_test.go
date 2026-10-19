package oidc

import (
	"time"

	"golang.org/x/time/rate"
)

func (v *Verifier) SetUnknownKeyRefreshEvery(d time.Duration) {
	v.unknownKeyRefresh = rate.NewLimiter(rate.Every(d), 1)
}

func (v *Verifier) SetKeySetFetchTimeout(d time.Duration) {
	v.keySetFetchTimeout = d
}
