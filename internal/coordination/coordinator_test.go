package coordination

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRepositoryCoordinatorAcquisitionAndReleaseOrdering(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	trace := &coordinatorTrace{}
	coordinator := newRepositoryCoordinator(coordinatorDependencies{
		prepare: func(path string) (PreparedControlNamespace, error) {
			trace.add("prepare")
			if path != fixture.identity.CanonicalPath {
				t.Fatalf("prepare path=%q want=%q", path, fixture.identity.CanonicalPath)
			}
			return fixture.prepared, nil
		},
		reserve: func(identity Identity) (processReservationResource, error) {
			trace.add("reserve")
			return &fakeProcessReservation{releaseFn: func() { trace.add("reservation release") }}, nil
		},
		acquireNative: func(prepared PreparedControlNamespace) (nativeLockResource, error) {
			trace.add("native acquire")
			return &fakeNativeLock{releaseFn: func() error {
				trace.add("native release")
				return nil
			}}, nil
		},
		publishOwner: func(prepared PreparedControlNamespace, owner Owner) error {
			trace.add("owner publish")
			return nil
		},
		removeOwner: func(prepared PreparedControlNamespace) error {
			trace.add("owner remove")
			return nil
		},
	})

	lease, err := coordinator.Acquire(context.Background(), fixture.identity, fixture.request)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease == nil {
		t.Fatal("Acquire returned nil Lease")
	}
	trace.require(t, []string{"prepare", "reserve", "native acquire", "owner publish"})

	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	trace.require(t, []string{
		"prepare", "reserve", "native acquire", "owner publish",
		"owner remove", "native release", "reservation release",
	})
}

func TestRepositoryCoordinatorAcquisitionFailureUnwind(t *testing.T) {
	prepareErr := errors.New("prepare failure")
	reserveErr := errors.New("reserve failure")
	nativeErr := ErrRepositoryBusy
	publishErr := errors.New("owner publication failure")
	nativeReleaseErr := errors.New("native release failure")

	tests := []struct {
		name      string
		configure func(*coordinatorDependencies, *coordinatorTrace)
		want      []string
		wantErrs  []error
	}{
		{
			name: "prepare failure stops acquisition",
			configure: func(dependencies *coordinatorDependencies, trace *coordinatorTrace) {
				dependencies.prepare = func(string) (PreparedControlNamespace, error) {
					trace.add("prepare")
					return PreparedControlNamespace{}, prepareErr
				}
			},
			want:     []string{"prepare"},
			wantErrs: []error{prepareErr},
		},
		{
			name: "reservation failure stops native acquisition",
			configure: func(dependencies *coordinatorDependencies, trace *coordinatorTrace) {
				dependencies.reserve = func(Identity) (processReservationResource, error) {
					trace.add("reserve")
					return nil, reserveErr
				}
			},
			want:     []string{"prepare", "reserve"},
			wantErrs: []error{reserveErr},
		},
		{
			name: "native failure releases reservation",
			configure: func(dependencies *coordinatorDependencies, trace *coordinatorTrace) {
				dependencies.acquireNative = func(PreparedControlNamespace) (nativeLockResource, error) {
					trace.add("native acquire")
					return nil, nativeErr
				}
			},
			want:     []string{"prepare", "reserve", "native acquire", "reservation release"},
			wantErrs: []error{nativeErr},
		},
		{
			name: "owner failure releases native then reservation",
			configure: func(dependencies *coordinatorDependencies, trace *coordinatorTrace) {
				dependencies.publishOwner = func(PreparedControlNamespace, Owner) error {
					trace.add("owner publish")
					return publishErr
				}
			},
			want: []string{
				"prepare", "reserve", "native acquire", "owner publish",
				"native release", "reservation release",
			},
			wantErrs: []error{publishErr},
		},
		{
			name: "owner and native cleanup failures are both preserved",
			configure: func(dependencies *coordinatorDependencies, trace *coordinatorTrace) {
				dependencies.publishOwner = func(PreparedControlNamespace, Owner) error {
					trace.add("owner publish")
					return publishErr
				}
				dependencies.acquireNative = func(PreparedControlNamespace) (nativeLockResource, error) {
					trace.add("native acquire")
					return &fakeNativeLock{releaseFn: func() error {
						trace.add("native release")
						return nativeReleaseErr
					}}, nil
				}
			},
			want: []string{
				"prepare", "reserve", "native acquire", "owner publish",
				"native release", "reservation release",
			},
			wantErrs: []error{publishErr, nativeReleaseErr},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCoordinatorFixture(t)
			trace := &coordinatorTrace{}
			dependencies := successfulCoordinatorDependencies(fixture.prepared, trace)
			test.configure(&dependencies, trace)
			coordinator := newRepositoryCoordinator(dependencies)

			lease, err := coordinator.Acquire(context.Background(), fixture.identity, fixture.request)
			if lease != nil {
				_ = lease.Release()
				t.Fatal("failed acquisition returned a Lease")
			}
			if err == nil {
				t.Fatal("failed acquisition returned nil error")
			}
			for _, wantErr := range test.wantErrs {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Acquire error=%v want errors.Is(%v)", err, wantErr)
				}
			}
			trace.require(t, test.want)
		})
	}
}

func TestRepositoryCoordinatorHonorsCanceledContextBeforeSideEffects(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	trace := &coordinatorTrace{}
	coordinator := newRepositoryCoordinator(successfulCoordinatorDependencies(fixture.prepared, trace))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lease, err := coordinator.Acquire(ctx, fixture.identity, fixture.request)
	if lease != nil {
		t.Fatal("canceled acquisition returned a Lease")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error=%v want context.Canceled", err)
	}
	trace.require(t, nil)
}

func TestRepositoryLeaseDiagnosticRemovalAndNativeFailureSemantics(t *testing.T) {
	ownerRemovalErr := errors.New("owner removal failure")
	nativeReleaseErr := errors.New("native release failure")

	tests := []struct {
		name             string
		ownerRemovalErr  error
		nativeReleaseErr error
		wantErrs         []error
	}{
		{name: "owner removal alone is non-fatal", ownerRemovalErr: ownerRemovalErr},
		{name: "native release is authoritative", nativeReleaseErr: nativeReleaseErr, wantErrs: []error{nativeReleaseErr}},
		{
			name:             "owner and native failures are joined",
			ownerRemovalErr:  ownerRemovalErr,
			nativeReleaseErr: nativeReleaseErr,
			wantErrs:         []error{ownerRemovalErr, nativeReleaseErr},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCoordinatorFixture(t)
			trace := &coordinatorTrace{}
			dependencies := successfulCoordinatorDependencies(fixture.prepared, trace)
			dependencies.removeOwner = func(PreparedControlNamespace) error {
				trace.add("owner remove")
				return test.ownerRemovalErr
			}
			dependencies.acquireNative = func(PreparedControlNamespace) (nativeLockResource, error) {
				trace.add("native acquire")
				return &fakeNativeLock{releaseFn: func() error {
					trace.add("native release")
					return test.nativeReleaseErr
				}}, nil
			}
			coordinator := newRepositoryCoordinator(dependencies)
			lease, err := coordinator.Acquire(context.Background(), fixture.identity, fixture.request)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}

			err = lease.Release()
			if len(test.wantErrs) == 0 && err != nil {
				t.Fatalf("Release error=%v want nil", err)
			}
			for _, wantErr := range test.wantErrs {
				if !errors.Is(err, wantErr) {
					t.Fatalf("Release error=%v want errors.Is(%v)", err, wantErr)
				}
			}
			trace.require(t, []string{
				"prepare", "reserve", "native acquire", "owner publish",
				"owner remove", "native release", "reservation release",
			})
		})
	}
}

func TestRepositoryLeaseConcurrentReleaseRunsLifecycleOnce(t *testing.T) {
	fixture := newCoordinatorFixture(t)
	trace := &coordinatorTrace{}
	coordinator := newRepositoryCoordinator(successfulCoordinatorDependencies(fixture.prepared, trace))
	lease, err := coordinator.Acquire(context.Background(), fixture.identity, fixture.request)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	const releasers = 32
	errorsByRelease := make(chan error, releasers)
	var workers sync.WaitGroup
	workers.Add(releasers)
	for range releasers {
		go func() {
			defer workers.Done()
			errorsByRelease <- lease.Release()
		}()
	}
	workers.Wait()
	close(errorsByRelease)
	for err := range errorsByRelease {
		if err != nil {
			t.Fatalf("concurrent Release: %v", err)
		}
	}

	trace.require(t, []string{
		"prepare", "reserve", "native acquire", "owner publish",
		"owner remove", "native release", "reservation release",
	})
}

type coordinatorFixture struct {
	identity Identity
	prepared PreparedControlNamespace
	request  Request
}

func newCoordinatorFixture(t *testing.T) coordinatorFixture {
	t.Helper()
	identity, err := ResolveIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("ResolveIdentity: %v", err)
	}
	controlDirectory, err := ControlDirectory(identity)
	if err != nil {
		t.Fatalf("ControlDirectory: %v", err)
	}
	owner, err := NewOwner(OperationStore, identity, "test-version", time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	return coordinatorFixture{
		identity: identity,
		prepared: PreparedControlNamespace{
			Identity:          identity,
			ControlDirectory:  controlDirectory,
			LockArtifactPath:  filepath.Join(controlDirectory, LockArtifactName),
			OwnerMetadataPath: filepath.Join(controlDirectory, OwnerMetadataName),
		},
		request: Request{Operation: OperationStore, Mode: ModeExclusive, Owner: owner},
	}
}

func successfulCoordinatorDependencies(
	prepared PreparedControlNamespace,
	trace *coordinatorTrace,
) coordinatorDependencies {
	return coordinatorDependencies{
		prepare: func(string) (PreparedControlNamespace, error) {
			trace.add("prepare")
			return prepared, nil
		},
		reserve: func(Identity) (processReservationResource, error) {
			trace.add("reserve")
			return &fakeProcessReservation{releaseFn: func() { trace.add("reservation release") }}, nil
		},
		acquireNative: func(PreparedControlNamespace) (nativeLockResource, error) {
			trace.add("native acquire")
			return &fakeNativeLock{releaseFn: func() error {
				trace.add("native release")
				return nil
			}}, nil
		},
		publishOwner: func(PreparedControlNamespace, Owner) error {
			trace.add("owner publish")
			return nil
		},
		removeOwner: func(PreparedControlNamespace) error {
			trace.add("owner remove")
			return nil
		},
	}
}

type fakeProcessReservation struct {
	releaseOnce sync.Once
	releaseFn   func()
}

func (reservation *fakeProcessReservation) release() {
	reservation.releaseOnce.Do(reservation.releaseFn)
}

type fakeNativeLock struct {
	releaseOnce sync.Once
	releaseFn   func() error
	releaseErr  error
}

func (lock *fakeNativeLock) release() error {
	lock.releaseOnce.Do(func() {
		lock.releaseErr = lock.releaseFn()
	})
	return lock.releaseErr
}

type coordinatorTrace struct {
	mu     sync.Mutex
	events []string
}

func (trace *coordinatorTrace) add(event string) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.events = append(trace.events, event)
}

func (trace *coordinatorTrace) require(t *testing.T, want []string) {
	t.Helper()
	trace.mu.Lock()
	got := append([]string(nil), trace.events...)
	trace.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("trace=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("trace=%v want=%v", got, want)
		}
	}
}
