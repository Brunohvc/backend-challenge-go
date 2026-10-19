package wager

// Kind is the type of a wagering transaction.
type Kind string

// The transaction kinds.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func parseExternalKind(s string) (Kind, bool) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, true
	}
	return "", false
}

func (k Kind) mustBeZero() bool {
	return k == KindLoss
}

func (k Kind) mustBePositive() bool {
	switch k {
	case KindOpening, KindBet, KindWin, KindRefund, KindRollback:
		return true
	}
	return false
}

func (k Kind) requiresReference() bool {
	return k == KindRefund || k == KindRollback
}

func (k Kind) forbidsReference() bool {
	return k == KindBet || k == KindLoss
}

func (k Kind) canReference(target Kind) bool {
	switch k {
	case KindRefund, KindWin:
		return target == KindBet
	case KindRollback:
		return target == KindBet || target == KindWin || target == KindRefund
	}
	return false
}

// Status is the position of a transaction in its lifecycle.
type Status string

// The transaction statuses.
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// IsTerminal reports whether no transition leaves s.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

// Origin says who started a transaction.
type Origin string

// The transaction origins.
const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)
