package recoveryexport

import (
	"context"
	"errors"
	"os"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

// BindingStatus is passive. A nil catalog selection deliberately reports only
// local evidence; it never claims database or ordinary-writer readiness.
func BindingStatus(ctx context.Context, directory string, c *Config) (result BindingResult, err error) {
	result.Status = "binding-mismatch"
	if ctx == nil {
		return result, ErrConfiguration
	}
	installation, e := openBindingDirectory(directory, true)
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, installation.close()) }()
	var d BindingDescriptor
	if e = readBindingFile(ctx, installation, bindingDescriptorName, BindingDescriptorMaxBytes, &d); e != nil {
		if _, x := installation.root.Lstat(bindingIntentName); x == nil {
			result.Status = "reconciliation-required"
		}
		return result, e
	}
	result.Descriptor = d
	if !validDescriptor(d) || !db.ValidIdentity("repo", d.Marker.RepositoryID) || !db.ValidIdentity("root", d.Marker.RootID) {
		return result, ErrBindingMismatch
	}
	if _, e = installation.root.Lstat(bindingIntentName); !os.IsNotExist(e) {
		result.Status = "reconciliation-required"
		return result, errors.Join(e, ErrBindingReconciliation)
	}
	path, e := decodeBindingPath(d.RootPathBase64)
	if e != nil {
		return result, e
	}
	root, e := openBindingDirectory(path, false)
	if e != nil {
		result.Status = "root-unavailable"
		return result, e
	}
	defer func() { err = errors.Join(err, root.close()) }()
	got, e := bindingSessionEvidence(root, false)
	if e != nil || got != d.Evidence {
		return result, errors.Join(e, ErrBindingMismatch)
	}
	control, e := openBindingDirectory(path+"/.coldkeep-control", true)
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, control.close()) }()
	var marker BindingMarker
	if e = readBindingFile(ctx, control, bindingMarkerName, BindingMarkerMaxBytes, &marker); e != nil || marker != d.Marker {
		return result, errors.Join(e, ErrBindingMismatch)
	}
	if c == nil {
		result.Status = "local-binding-matches; catalog-unchecked"
		return result, nil
	}
	if bindingLocator(*c) != d.Locator {
		return result, ErrBindingMismatch
	}
	if path != c.PayloadRoot {
		return result, ErrBindingMismatch
	}
	if e = noRetainedAuthority(path); e != nil {
		result.Status = "recovery-only; maintenance refused"
		return result, e
	}
	_, e = Inspect(ctx, *c, DefaultLimits(), func(_ context.Context, i *Inventory) error {
		actual, x := currentBindingDescriptor(i, bindingLocator(*c))
		if x != nil {
			return x
		}
		if actual != d {
			return ErrBindingMismatch
		}
		return nil
	})
	if e != nil {
		result.Status = "catalog-unavailable"
		if errors.Is(e, ErrKeyUnavailable) {
			result.Status = "key-unavailable"
		}
		if errors.Is(e, ErrBindingMismatch) || errors.Is(e, ErrUnsupported) || errors.Is(e, ErrCorrupt) {
			result.Status = "binding-mismatch"
		}
		return result, e
	}
	if e = errors.Join(root.check(), control.check(), installation.check()); e != nil {
		return result, e
	}
	result.Status = "bounded-maintenance-ready; ordinary writers refused"
	return result, nil
}

// SourceBindingStatus never enumerates membership or writes a source marker.
func SourceBindingStatus(ctx context.Context, directory string, c Config, sourceID string) (result BindingResult, err error) {
	result, err = BindingStatus(ctx, directory, &c)
	if err != nil {
		return result, err
	}
	result.SourceID = sourceID
	_, err = Inspect(ctx, c, DefaultLimits(), func(_ context.Context, i *Inventory) error {
		var revision int64
		for _, h := range i.Catalog["source_binding_head"] {
			if h.s("source_id") == sourceID {
				revision = h.n("revision")
			}
		}
		for _, r := range i.Catalog["source_binding_revision"] {
			if r.s("source_id") != sourceID || r.n("revision") != revision {
				continue
			}
			result.SourceRevision = revision
			path, e := decodeBindingPath(r.s("path_base64"))
			if e != nil {
				return e
			}
			d, e := openBindingDirectory(path, false)
			if e != nil {
				result.Status = "source-unavailable"
				return e
			}
			got, e := bindingSessionEvidence(d, true)
			e = errors.Join(e, d.close())
			if e != nil {
				return e
			}
			want, e := bindingRowEvidence(r, true)
			if e != nil {
				return e
			}
			if got != want {
				result.Status = "source-binding-mismatch"
				return ErrBindingMismatch
			}
			result.Status = "source-available; capture disabled"
			return nil
		}
		return ErrBindingMismatch
	})
	return result, err
}
