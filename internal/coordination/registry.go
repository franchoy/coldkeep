package coordination

import (
	"fmt"
	"sync"
)

// processRegistry reserves canonical repository identities within one process.
// It does not represent native repository ownership.
type processRegistry struct {
	mu   sync.Mutex
	held map[string]*processReservation
}

type processReservation struct {
	registry     *processRegistry
	identityHash string
	releaseOnce  sync.Once
}

func (registry *processRegistry) reserve(identity Identity) (*processReservation, error) {
	if err := ValidateIdentity(identity); err != nil {
		return nil, err
	}
	reservation := &processReservation{
		registry:     registry,
		identityHash: identity.Hash,
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.held == nil {
		registry.held = make(map[string]*processReservation)
	}
	if _, exists := registry.held[identity.Hash]; exists {
		return nil, fmt.Errorf("%w: repository identity is already reserved in this process", ErrNestedRepositoryAcquisition)
	}
	registry.held[identity.Hash] = reservation
	return reservation, nil
}

func (reservation *processReservation) release() {
	if reservation == nil || reservation.registry == nil {
		return
	}
	reservation.releaseOnce.Do(func() {
		reservation.registry.mu.Lock()
		defer reservation.registry.mu.Unlock()
		if reservation.registry.held[reservation.identityHash] == reservation {
			delete(reservation.registry.held, reservation.identityHash)
		}
	})
}
