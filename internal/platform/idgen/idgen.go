// Package idgen is the only place new identifiers are generated.
package idgen

import (
	"fmt"

	"github.com/google/uuid"
)

// V7 generates UUIDv7 identifiers.
type V7 struct{}

// New returns a new UUIDv7.
func (V7) New() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Errorf("idgen: generate uuid v7: %w", err))
	}
	return id
}
