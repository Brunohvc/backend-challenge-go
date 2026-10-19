package outboxrelay

var NewWithSuffix = newRelay

func (r *Relay) Owner() string { return r.owner }
