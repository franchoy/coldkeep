package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/generation"
)

func TestRecoveryReadLifetime(t *testing.T) {
	f, b, _, sources, gens := generationFixture(t)
	q := generationReadRequest(t, f, b)
	prepareReadFixture(t, f, q)
	request := readRequest(q, sources[0], gens[0], generation.QueryDetail)
	for _, kind := range []string{"rollback", "driver-close-error", "cancellation", "callback-failure"} {
		t.Run(kind, func(t *testing.T) {
			var closeErr error
			if kind == "driver-close-error" {
				closeErr = errors.New("injected physical close error")
			}
			gates := newCleanupDriver(t, nil, closeErr)
			h := &generationReadHooks{binding: &bindingHooks{cut: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			var result ReadViewResult
			var opErr error
			go func() {
				result, opErr = withGenerationReadView(ctx, f.config, q, DefaultLimits(), func(ctx context.Context, r generation.Reader) error {
					_, e := r.Read(ctx, request)
					if e != nil {
						return e
					}
					if kind == "callback-failure" {
						return errors.New("callback failed")
					}
					return nil
				}, h)
				close(done)
			}()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			select {
			case <-gates.rollbackStarted.ch:
			case <-done:
				t.Fatalf("view returned early: %v", opErr)
			case <-time.After(5 * time.Second):
				t.Fatal("rollback barrier timeout")
			}
			bindingContenders(t, f)
			if kind == "cancellation" {
				cancel()
			}
			select {
			case <-done:
				t.Fatal("lease escaped rollback")
			default:
			}
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			bindingContenders(t, f)
			select {
			case <-done:
				t.Fatal("lease escaped driver close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if (opErr == nil) != (kind == "rollback") || result.Complete != (kind == "rollback") {
				t.Fatal(result, opErr)
			}
		})
	}
	t.Run("callback-return-races-page", func(t *testing.T) {
		entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		h := &generationReadHooks{query: func(query string, _ []any) {
			if strings.Contains(query, "FROM public.capture_generation") {
				once.Do(func() { close(entered); <-release })
			}
		}}
		var escaped generation.Reader
		var methodErr, outerErr error
		var result ReadViewResult
		methodDone := make(chan struct{})
		go func() {
			result, outerErr = withGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, r generation.Reader) error {
				escaped = r
				go func() { _, methodErr = r.Read(ctx, request); close(methodDone) }()
				<-entered
				if _, e := r.Read(ctx, request); !errors.Is(e, ErrReadViewBusy) {
					return errors.New("simultaneous call admitted")
				}
				return nil
			}, h)
			close(returned)
		}()
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
			awaitCleanup(t, returned)
		}()
		awaitCleanup(t, entered)
		bindingContenders(t, f)
		select {
		case <-returned:
			t.Fatal("outer returned with admitted page outstanding")
		default:
		}
		close(release)
		awaitCleanup(t, methodDone)
		awaitCleanup(t, returned)
		if methodErr == nil || outerErr != nil || !result.Complete {
			t.Fatal(result, methodErr, outerErr)
		}
		if _, e := escaped.Read(context.Background(), request); !errors.Is(e, ErrReadViewClosed) {
			t.Fatal("escaped reader used SQL", e)
		}
	})
	t.Run("cancelled-method", func(t *testing.T) {
		_, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, r generation.Reader) error {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, e := r.Read(cancelled, request); !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			_, e := r.Read(ctx, request)
			return e
		})
		must(t, e)
	})
	for _, kind := range []string{"descriptor-substitution", "new-intent", "lost-snapshot"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(q.InstallationDirectory, bindingDescriptorName)
			original, e := os.ReadFile(path)
			must(t, e)
			defer func() {
				must(t, os.WriteFile(path, original, 0600))
				_ = os.Remove(filepath.Join(q.InstallationDirectory, bindingIntentName))
			}()
			r, e := WithGenerationReadView(context.Background(), f.config, q, DefaultLimits(), func(ctx context.Context, reader generation.Reader) error {
				switch kind {
				case "descriptor-substitution":
					must(t, os.WriteFile(path, []byte(`{}`), 0600))
				case "new-intent":
					must(t, os.WriteFile(filepath.Join(q.InstallationDirectory, bindingIntentName), []byte(`{}`), 0600))
				case "lost-snapshot":
					v := reader.(*generationReadView)
					must(t, v.tx.Rollback())
				}
				if _, e := reader.Read(ctx, request); e == nil {
					t.Fatal("lost ownership/snapshot succeeded")
				}
				return nil
			})
			if e == nil || r.Complete {
				t.Fatal(r, e)
			}
		})
	}
}

func TestRecoveryReadIndexJoinedCleanup(t *testing.T) {
	f, b, _, _, _ := generationFixture(t)
	q := generationReadRequest(t, f, b)
	for _, kind := range []string{"rollback", "close-error", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			var closeErr error
			if kind == "close-error" {
				closeErr = errors.New("index close failed")
			}
			gates := newCleanupDriver(t, nil, closeErr)
			h := &bindingHooks{migration: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return gatedCatalogConnector{c, gates} }}, event: func(stage string, _ *sql.Tx) error {
				if stage == "before-commit" {
					return errors.New("index transaction interrupted")
				}
				return nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			o := exportOptions(t)
			var r ReadIndexResult
			var opErr error
			go func() { r, opErr = prepareGenerationReadIndex(ctx, f.config, q, o, h); close(done) }()
			defer func() { gates.allowRollback.fire(); gates.allowClose.fire(); awaitCleanup(t, done) }()
			select {
			case <-gates.rollbackStarted.ch:
			case <-done:
				t.Fatalf("early index return %v", opErr)
			case <-time.After(5 * time.Second):
				t.Fatal("index rollback barrier timeout")
			}
			bindingContenders(t, f)
			if kind == "cancel" {
				cancel()
			}
			gates.allowRollback.fire()
			awaitCleanup(t, gates.closeStarted.ch)
			bindingContenders(t, f)
			select {
			case <-done:
				t.Fatal("index lease escaped physical close")
			default:
			}
			gates.allowClose.fire()
			awaitCleanup(t, done)
			if opErr == nil || r.Complete {
				t.Fatal(r, opErr)
			}
		})
	}
	t.Run("lost-commit-response", func(t *testing.T) {
		o := exportOptions(t)
		r, e := prepareGenerationReadIndex(context.Background(), f.config, q, o, &bindingHooks{migration: &hooks{wrapConnector: func(c driver.Connector) driver.Connector { return lostCommitConnector{c} }}})
		if e == nil || r.Complete || !strings.Contains(e.Error(), "lost actual commit response") {
			t.Fatal(r, e)
		}
		r, e = PrepareGenerationReadIndex(context.Background(), f.config, q, o)
		must(t, e)
		if !r.Complete || !r.Reconciled {
			t.Fatal(r)
		}
	})
}
