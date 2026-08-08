package coordination

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestProcessRegistryRejectsSequentialNestedReservationAndAllowsReacquire(t *testing.T) {
	registry := &processRegistry{}
	identity := mustIdentity(t, t.TempDir())
	reservation, err := registry.reserve(identity)
	if err != nil {
		t.Fatalf("reserve identity: %v", err)
	}
	if _, err := registry.reserve(identity); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("nested reservation error=%v", err)
	}
	reservation.release()
	reacquired, err := registry.reserve(identity)
	if err != nil {
		t.Fatalf("reserve identity after release: %v", err)
	}
	reacquired.release()
}

func TestProcessRegistryAllowsExactlyOneConcurrentSameIdentityReservation(t *testing.T) {
	const contenders = 32
	registry := &processRegistry{}
	identity := mustIdentity(t, t.TempDir())
	start := make(chan struct{})
	results := make(chan reservationResult, contenders)
	var workers sync.WaitGroup
	workers.Add(contenders)
	for range contenders {
		go func() {
			defer workers.Done()
			<-start
			reservation, err := registry.reserve(identity)
			results <- reservationResult{reservation: reservation, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	successes := 0
	nested := 0
	var winner *processReservation
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			winner = result.reservation
		case errors.Is(result.err, ErrNestedRepositoryAcquisition):
			nested++
		default:
			t.Fatalf("unexpected reservation error: %v", result.err)
		}
	}
	if successes != 1 || nested != contenders-1 {
		t.Fatalf("successes=%d nested=%d want=1/%d", successes, nested, contenders-1)
	}
	winner.release()
}

func TestProcessRegistryAllowsConcurrentDifferentIdentities(t *testing.T) {
	const repositories = 12
	registry := &processRegistry{}
	identities := make([]Identity, repositories)
	for i := range identities {
		identities[i] = mustIdentity(t, t.TempDir())
	}

	start := make(chan struct{})
	results := make(chan reservationResult, repositories)
	var workers sync.WaitGroup
	workers.Add(repositories)
	for _, identity := range identities {
		go func(identity Identity) {
			defer workers.Done()
			<-start
			reservation, err := registry.reserve(identity)
			results <- reservationResult{reservation: reservation, err: err}
		}(identity)
	}
	close(start)
	workers.Wait()
	close(results)

	reservations := make([]*processReservation, 0, repositories)
	for result := range results {
		if result.err != nil {
			t.Fatalf("reserve distinct identity: %v", result.err)
		}
		reservations = append(reservations, result.reservation)
	}
	if len(reservations) != repositories {
		t.Fatalf("distinct reservations=%d want=%d", len(reservations), repositories)
	}
	for _, reservation := range reservations {
		reservation.release()
	}
}

func TestProcessReservationReleaseIsIdempotentAndCannotRemoveSuccessor(t *testing.T) {
	registry := &processRegistry{}
	identity := mustIdentity(t, t.TempDir())
	first, err := registry.reserve(identity)
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	const releasers = 32
	var workers sync.WaitGroup
	workers.Add(releasers)
	for range releasers {
		go func() {
			defer workers.Done()
			first.release()
		}()
	}
	workers.Wait()

	second, err := registry.reserve(identity)
	if err != nil {
		t.Fatalf("reserve successor: %v", err)
	}
	first.release()
	if _, err := registry.reserve(identity); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("old release removed successor reservation, error=%v", err)
	}
	second.release()
}

func TestProcessRegistryCanonicalAliasesCollide(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "repository")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("create repository: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	direct := mustIdentity(t, target)
	throughAlias := mustIdentity(t, alias)

	registry := &processRegistry{}
	reservation, err := registry.reserve(direct)
	if err != nil {
		t.Fatalf("reserve direct identity: %v", err)
	}
	defer reservation.release()
	if _, err := registry.reserve(throughAlias); !errors.Is(err, ErrNestedRepositoryAcquisition) {
		t.Fatalf("canonical alias reservation error=%v", err)
	}
}

type reservationResult struct {
	reservation *processReservation
	err         error
}
