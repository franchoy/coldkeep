package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"golang.org/x/sys/unix"
)

type BindingOptions struct {
	Request    BindingRequest `json:"request"`
	PlanSHA256 string         `json:"plan_sha256"`
	Export     ExportOptions  `json:"export"`
}

type bindingHooks struct {
	event          func(string, *sql.Tx) error
	cut, migration *hooks
	export         *exportHooks
}

func (h *bindingHooks) at(s string, tx *sql.Tx) error {
	if h != nil && h.event != nil {
		return h.event(s, tx)
	}
	return nil
}

type bindingSession struct {
	ctx                                 context.Context
	c                                   Config
	r                                   resolved
	installation, root, control, source *bindingDirectory
	lease                               coordination.ExistingLease
	reservation                         *importReservation
	evidence, sourceEvidence            BindingEvidence
	h                                   *bindingHooks
}

func validBindingRequest(q BindingRequest) bool {
	if !validBindingID("bind", q.OperationID) || q.ExpectedRevision < 0 || q.ExpectedRevision >= 20000 || q.ExpectedSourceRevision < 0 {
		return false
	}
	switch q.Action {
	case BindingInitial:
		return q.ExpectedSchema == 18 && q.ExpectedRevision == 0 && q.SourceID == "" && q.SourcePathBase64 == "" && q.ExpectedSourceRevision == 0
	case BindingRelocate, BindingReconnect:
		return q.ExpectedSchema == 19 && q.ExpectedRevision > 0 && q.SourceID == "" && q.SourcePathBase64 == "" && q.ExpectedSourceRevision == 0
	case BindingSourceRegister:
		return q.ExpectedSchema == 19 && q.ExpectedRevision > 0 && db.ValidIdentity("source", q.SourceID) && q.SourcePathBase64 != "" && q.ExpectedSourceRevision == 0
	case BindingSourceRebind:
		return q.ExpectedSchema == 19 && q.ExpectedRevision > 0 && db.ValidIdentity("source", q.SourceID) && q.SourcePathBase64 != "" && q.ExpectedSourceRevision > 0
	case BindingSourceDisable:
		return q.ExpectedSchema == 19 && q.ExpectedRevision > 0 && db.ValidIdentity("source", q.SourceID) && q.SourcePathBase64 == "" && q.ExpectedSourceRevision > 0
	default:
		return false
	}
}
func openBindingSession(ctx context.Context, c Config, q BindingRequest, checkpoint string, h *bindingHooks) (s *bindingSession, err error) {
	if ctx == nil || !validBindingRequest(q) {
		return nil, ErrConfiguration
	}
	return openMaintenanceSession(ctx, c, q.InstallationDirectory, q.SourcePathBase64, checkpoint, h)
}

// The shared lifetime owns installation, canonical root and database aliases.
// Request semantics remain with each separately versioned maintenance protocol.
func openMaintenanceSession(ctx context.Context, c Config, installation, sourcePathBase64, checkpoint string, h *bindingHooks) (s *bindingSession, err error) {
	if _, e := EncodeBindingPath(c.PayloadRoot); e != nil {
		return nil, e
	}
	s = &bindingSession{ctx: ctx, c: c, h: h}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.close())
			s = nil
		}
	}()
	var e error
	s.installation, e = openBindingDirectory(installation, true)
	if e != nil {
		return s, fmt.Errorf("binding installation directory: %w", e)
	}
	if e = unix.Flock(int(s.installation.file.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return s, errors.Join(coordination.ErrRepositoryBusy, e)
	}
	s.r, e = resolve(ctx, c)
	if e != nil {
		return s, fmt.Errorf("binding explicit configuration: %w", e)
	}
	s.c.PayloadRoot = s.r.identity.CanonicalPath
	s.lease, e = coordination.AcquireExisting(ctx, s.r.identity)
	if e != nil {
		return s, fmt.Errorf("binding existing ownership: %w", e)
	}
	if e = noRetainedAuthority(s.r.identity.CanonicalPath); e != nil {
		return s, e
	}
	s.root, e = openBindingDirectory(s.r.identity.CanonicalPath, false)
	if e != nil {
		return s, fmt.Errorf("binding root directory: %w", e)
	}
	s.control, e = openBindingDirectory(filepath.Join(s.root.path, ".coldkeep-control"), true)
	if e != nil {
		return s, fmt.Errorf("binding control directory: %w", e)
	}
	s.evidence, e = bindingSessionEvidence(s.root, false)
	if e != nil {
		return s, fmt.Errorf("binding root evidence: %w", e)
	}
	sourcePath := ""
	if sourcePathBase64 != "" {
		sourcePath, e = decodeBindingPath(sourcePathBase64)
		if e != nil {
			return s, e
		}
		if pathContains(s.root.path, sourcePath) || pathContains(sourcePath, s.root.path) {
			return s, ErrConfiguration
		}
		s.source, e = openBindingDirectory(sourcePath, false)
		if e != nil {
			return s, e
		}
		s.sourceEvidence, e = bindingSessionEvidence(s.source, true)
		if e != nil {
			return s, e
		}
	}
	if e = validateInstallationLocation(s.installation, s.root, sourcePath, checkpoint); e != nil {
		return s, fmt.Errorf("binding installation placement: %w", e)
	}
	s.reservation, e = reserveImport(ctx, s.r)
	if e != nil {
		return s, e
	}
	return s, s.check()
}
func (s *bindingSession) check() error {
	if e := errors.Join(s.ctx.Err(), s.lease.Check(), s.installation.check(), s.root.check(), s.control.check()); e != nil {
		return e
	}
	now, e := bindingSessionEvidence(s.root, false)
	if e != nil {
		return e
	}
	if now != s.evidence {
		return ErrBindingMismatch
	}
	if s.source != nil {
		now, e = bindingSessionEvidence(s.source, true)
		if e != nil {
			return e
		}
		if now != s.sourceEvidence {
			return ErrBindingMismatch
		}
	}
	if e = noRetainedAuthority(s.root.path); e != nil {
		return e
	}
	return s.reservation.check(s.ctx)
}
func (s *bindingSession) close() error {
	if s == nil {
		return nil
	}
	e := s.h.at("cleanup", nil)
	e = errors.Join(e, s.source.close(), s.control.close(), s.root.close())
	if s.reservation != nil {
		e = errors.Join(e, s.reservation.close())
	}
	if s.lease != nil {
		e = errors.Join(e, s.lease.Release())
	}
	e = errors.Join(e, s.installation.close())
	clear(s.r.key)
	return e
}
func (s *bindingSession) inventory(l Limits) (i *Inventory, err error) {
	var h *hooks
	if s.h != nil {
		h = s.h.cut
	}
	r, e := inspectOwnedCut(s.ctx, s.c, s.r, l, func(_ context.Context, _ resolved, _ *sql.Tx, got *Inventory) error { i = got; return nil }, h, ownedCut{s.r.identity, s.lease})
	if e != nil {
		return nil, e
	}
	if !r.Complete {
		return nil, ErrCorrupt
	}
	return i, s.check()
}
func (s *bindingSession) preview(q BindingRequest, i *Inventory, reconciling ...bool) (result BindingResult, err error) {
	result.Status = "preview; ordinary writers refused"
	if i.CatalogVersion != q.ExpectedSchema {
		return result, ErrBindingMismatch
	}
	// Previously configured sources remain protected locations even when offline.
	for _, row := range i.Catalog["source_binding_revision"] {
		path, e := decodeBindingPath(row.s("path_base64"))
		if e != nil {
			return result, e
		}
		if pathContains(path, s.installation.path) || pathContains(s.installation.path, path) {
			return result, ErrConfiguration
		}
	}
	if len(reconciling) == 0 {
		if e := bindingFileAbsent(s.installation, bindingIntentName); e != nil {
			return result, errors.Join(ErrBindingReconciliation, e)
		}
		if e := bindingFileAbsent(s.installation, bindingIntentName+".pending"); e != nil {
			return result, errors.Join(ErrBindingReconciliation, e)
		}
	}
	path, e := EncodeBindingPath(s.root.path)
	if e != nil {
		return result, e
	}
	if q.Action == BindingInitial {
		if e = errors.Join(bindingFileAbsent(s.control, bindingMarkerName), bindingFileAbsent(s.installation, bindingDescriptorName)); e != nil {
			return result, e
		}
		result.Descriptor = BindingDescriptor{Kind: "coldkeep-installation/v1", Locator: bindingLocator(s.c), RootPathBase64: path, Evidence: s.evidence, Marker: BindingMarker{Kind: "coldkeep-root-marker/v1", RepositoryID: i.Catalog["repository_identity"][0].s("repository_id"), RootID: i.Catalog["storage_root_identity"][0].s("root_id"), Revision: 1, OperationID: q.OperationID}}
	} else {
		old, e := currentBindingDescriptor(i, bindingLocator(s.c))
		if e != nil {
			return result, e
		}
		if old.Marker.Revision != q.ExpectedRevision {
			return result, ErrBindingMismatch
		}
		var d BindingDescriptor
		var m BindingMarker
		if e = errors.Join(readBindingFile(s.ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &d), readBindingFile(s.ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &m)); e != nil {
			return result, e
		}
		if !validDescriptor(d) || d != old || m != old.Marker {
			return result, ErrBindingMismatch
		}
		switch q.Action {
		case BindingRelocate, BindingReconnect:
			if !sameBindingObjects(old.Evidence, s.evidence) {
				return result, ErrBindingMismatch
			}
			if q.Action == BindingRelocate && (old.Evidence != s.evidence || path == old.RootPathBase64) {
				return result, ErrBindingMismatch
			}
			oldPath, e := decodeBindingPath(old.RootPathBase64)
			if e != nil {
				return result, e
			}
			if info, x := os.Stat(oldPath); x == nil && !os.SameFile(info, s.root.info) {
				return result, ErrBindingMismatch
			} else if x != nil && !os.IsNotExist(x) {
				return result, x
			}
		default:
			if path != old.RootPathBase64 || s.evidence != old.Evidence {
				return result, ErrBindingMismatch
			}
		}
		result.Prior = &old
		result.Descriptor = old
		result.Descriptor.RootPathBase64 = path
		result.Descriptor.Evidence = s.evidence
		result.Descriptor.Marker.Revision++
		result.Descriptor.Marker.OperationID = q.OperationID
		result.Descriptor.Marker.PlanSHA256 = ""
	}
	if q.SourceID != "" {
		result.SourceID = q.SourceID
		result.SourceRevision = q.ExpectedSourceRevision + 1
		var source, prior Row
		for _, r := range i.Catalog["configured_source"] {
			if r.s("source_id") == q.SourceID {
				source = r
			}
		}
		for _, r := range i.Catalog["source_binding_revision"] {
			if r.s("source_id") == q.SourceID && r.n("revision") == q.ExpectedSourceRevision {
				prior = r
			}
		}
		if q.Action == BindingSourceRegister {
			if source != nil || len(i.Catalog["configured_source"]) >= BindingMaxSources || q.SourceID == i.Catalog["source_identity"][0].s("source_id") {
				return result, ErrBindingMismatch
			}
			result.Continuity = "new"
		} else {
			var head int64
			for _, r := range i.Catalog["source_binding_head"] {
				if r.s("source_id") == q.SourceID {
					head = r.n("revision")
				}
			}
			if source == nil || source.b("disabled") || prior == nil || head != q.ExpectedSourceRevision {
				return result, ErrBindingMismatch
			}
			if q.Action == BindingSourceDisable {
				result.Continuity = "disabled"
			} else {
				old, e := bindingRowEvidence(prior, true)
				if e != nil {
					return result, e
				}
				result.Continuity = "changed"
				if old == s.sourceEvidence {
					result.Continuity = "same-object"
				}
			}
		}
	}
	result.PlanSHA256 = bindingDigest(struct {
		Request BindingRequest
		Catalog string
		Result  BindingResult
		Source  BindingEvidence
	}{q, adoptionPlan(s.c, i), result, s.sourceEvidence})
	result.Descriptor.Marker.PlanSHA256 = result.PlanSHA256
	return result, nil
}

func PreviewBinding(ctx context.Context, c Config, q BindingRequest, l Limits) (result BindingResult, err error) {
	if ctx == nil || !l.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, l.Duration)
	defer cancel()
	s, e := openBindingSession(ctx, c, q, "", nil)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil {
			result.Status = "preview refused"
		}
	}()
	i, e := s.inventory(l)
	if e != nil {
		return result, e
	}
	return s.preview(q, i)
}
func ApplyBinding(ctx context.Context, c Config, o BindingOptions) (BindingResult, error) {
	return applyBinding(ctx, c, o, nil)
}

// ReconcileBinding deliberately uses the same fully bound request and plan.
func ReconcileBinding(ctx context.Context, c Config, o BindingOptions) (BindingResult, error) {
	return applyBinding(ctx, c, o, nil)
}
func applyBinding(ctx context.Context, c Config, o BindingOptions, h *bindingHooks) (result BindingResult, err error) {
	result.Status = "incomplete; binding not committed"
	if ctx == nil || !o.Export.Limits.valid() || !o.Export.InventoryLimits.valid() || !safeDigest.MatchString(o.PlanSHA256) {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, min(o.Export.Limits.Duration, o.Export.InventoryLimits.Duration))
	defer cancel()
	s, e := openBindingSession(ctx, c, o.Request, o.Export.Destination, h)
	if e != nil {
		return result, e
	}
	committed := false
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil && committed {
			result.Status = "committed; reconciliation-required"
			err = errors.Join(err, ErrBindingReconciliation)
		} else if err != nil {
			result.Status = "incomplete; binding not committed"
		}
	}()
	i, e := s.inventory(o.Export.InventoryLimits)
	if e != nil {
		return result, e
	}
	var intent bindingIntent
	intentErr := readBindingFile(ctx, s.installation, bindingIntentName, BindingIntentMaxBytes, &intent)
	var existing Row
	for _, op := range i.Catalog["binding_operation"] {
		if op.s("operation_id") == o.Request.OperationID {
			existing = op
		}
	}
	if existing != nil {
		committed = true
		if existing.s("request_sha256") != bindingDigest(o.Request) || existing.s("plan_sha256") != o.PlanSHA256 || existing.s("action") != string(o.Request.Action) || existing.n("from_revision") != o.Request.ExpectedRevision {
			return result, ErrBindingMismatch
		}
		d, e := currentBindingDescriptor(i, bindingLocator(s.c))
		if e != nil {
			return result, e
		}
		if d.Marker.OperationID != o.Request.OperationID {
			return result, ErrBindingMismatch
		}
		if os.IsNotExist(intentErr) {
			var stored BindingDescriptor
			var marker BindingMarker
			if e = errors.Join(readBindingFile(ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &stored), readBindingFile(ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &marker)); e != nil {
				return result, e
			}
			path, _ := EncodeBindingPath(s.root.path)
			if stored != d || marker != d.Marker || d.Evidence != s.evidence || d.RootPathBase64 != path {
				return result, ErrBindingMismatch
			}
			result = BindingResult{Status: "complete; bounded maintenance only", PlanSHA256: o.PlanSHA256, Descriptor: d, Generation: existing.s("generation"), ManifestSHA256: existing.s("manifest_sha256")}

			for _, row := range i.Catalog["source_binding_revision"] {
				if row.s("operation_id") == o.Request.OperationID {
					if o.Request.Action != BindingSourceDisable {
						evidence, x := bindingRowEvidence(row, true)
						if x != nil || evidence != s.sourceEvidence || row.s("path_base64") != o.Request.SourcePathBase64 {
							return result, errors.Join(x, ErrBindingMismatch)
						}
					}
					result.SourceID = row.s("source_id")
					result.SourceRevision = row.n("revision")
					result.Continuity = row.s("continuity")
				}
			}
			return result, s.check()
		}
		if intentErr != nil {
			return result, intentErr
		}
		if !matchingBindingIntent(intent, o) || intent.Result.Descriptor != d {
			return result, ErrBindingMismatch
		}
		if intent.Result.Generation != existing.s("generation") || intent.Result.ManifestSHA256 != existing.s("manifest_sha256") {
			return result, ErrBindingMismatch
		}
		if o.Request.ExpectedRevision == 0 {
			if intent.Result.Prior != nil {
				return result, ErrBindingMismatch
			}
		} else {
			prior, x := bindingDescriptorAt(i, bindingLocator(s.c), o.Request.ExpectedRevision)
			if x != nil || intent.Result.Prior == nil || *intent.Result.Prior != prior {
				return result, errors.Join(x, ErrBindingMismatch)
			}
		}
		var sourceID, continuity string
		var sourceRevision int64
		for _, row := range i.Catalog["source_binding_revision"] {
			if row.s("operation_id") == o.Request.OperationID {
				sourceID, continuity, sourceRevision = row.s("source_id"), row.s("continuity"), row.n("revision")
			}
		}
		if intent.Result.SourceID != sourceID || intent.Result.SourceRevision != sourceRevision || intent.Result.Continuity != continuity {
			return result, ErrBindingMismatch
		}
		result = intent.Result
	} else {
		if intentErr == nil {
			if !matchingBindingIntent(intent, o) || intent.PriorPlan != adoptionPlan(s.c, i) || intent.Result.Descriptor.Evidence != s.evidence || intent.SourceEvidence != s.sourceEvidence {
				return result, ErrBindingMismatch
			}

			expected, e := s.preview(o.Request, i, true)
			if e != nil || expected.PlanSHA256 != o.PlanSHA256 {
				return result, errors.Join(e, ErrBindingMismatch)
			}
			if o.Request.Action == BindingInitial {
				expected.Descriptor.Marker.InstallationID = intent.Result.Descriptor.Marker.InstallationID
			}
			if o.Request.Action == BindingInitial || o.Request.Action == BindingReconnect {
				expected.Descriptor.Marker.Incarnation = intent.Result.Descriptor.Marker.Incarnation
			}
			expected.Generation = intent.Result.Generation
			expected.ManifestSHA256 = intent.Result.ManifestSHA256
			if bindingDigest(expected) != bindingDigest(intent.Result) {
				return result, ErrBindingMismatch
			}
			result = intent.Result
		} else if !os.IsNotExist(intentErr) {
			return result, intentErr
		} else {
			result, e = s.preview(o.Request, i)
			if e != nil {
				return result, e
			}
			if result.PlanSHA256 != o.PlanSHA256 {
				return result, ErrBindingMismatch
			}
			switch o.Request.Action {
			case BindingInitial:
				result.Descriptor.Marker.InstallationID, e = bindingID("install")
				if e != nil {
					return result, e
				}
				result.Descriptor.Marker.Incarnation, e = bindingID("incarnation")
				if e != nil {
					return result, e
				}
			case BindingReconnect:
				result.Descriptor.Marker.Incarnation, e = bindingID("incarnation")
				if e != nil {
					return result, e
				}
			}
			if e = h.at("before-checkpoint", nil); e != nil {
				return result, e
			}
			owned := ownedCut{s.r.identity, s.lease}
			var eh *exportHooks
			if h != nil {
				eh = h.export
			}
			checkpoint, e := exportOwned(ctx, s.c, o.Export, eh, &s.r, &owned)
			result.Generation = checkpoint.Generation
			if e != nil || !checkpoint.Complete {
				return result, errors.Join(e, ErrPackage)
			}
			intent = bindingIntent{Kind: "coldkeep-binding-intent/v1", Request: o.Request, PlanSHA256: o.PlanSHA256, PriorPlan: adoptionPlan(s.c, i), Result: result, SourceEvidence: s.sourceEvidence}
			intent.CheckpointBase64, e = EncodeBindingPath(o.Export.Destination)
			if e != nil {
				return result, e
			}
			digest, e := checkBindingCheckpoint(ctx, o.Export, i.CatalogVersion, result.Generation, s.r.identity.Hash)
			if e != nil {
				return result, e
			}
			result.ManifestSHA256 = digest
			intent.Result = result
			if e = h.at("before-intent", nil); e != nil {
				return result, e
			}
			if e = publishBindingFile(ctx, s.installation, bindingIntentName, intent, BindingIntentMaxBytes, s.check, h); e != nil {
				return result, e
			}
			if e = h.at("after-intent", nil); e != nil {
				return result, e
			}
		}
		digest, e := checkBindingCheckpoint(ctx, o.Export, o.Request.ExpectedSchema, result.Generation, s.r.identity.Hash)
		if e != nil || digest != result.ManifestSHA256 {
			return result, errors.Join(e, ErrPackage)
		}
		fresh, e := s.inventory(o.Export.InventoryLimits)
		if e != nil {
			return result, e
		}
		if adoptionPlan(s.c, fresh) != intent.PriorPlan {
			return result, ErrBindingMismatch
		}
		// This method joins physical transaction/driver cleanup before returning.
		committed, e = s.commit(o, intent)
		if e != nil {
			return result, e
		}
		if e = h.at("after-commit", nil); e != nil {
			return result, e
		}
		i, e = s.inventory(o.Export.InventoryLimits)
		if e != nil {
			return result, e
		}
		actual, e := currentBindingDescriptor(i, bindingLocator(s.c))
		if e != nil || actual != intent.Result.Descriptor {
			return result, errors.Join(e, ErrBindingMismatch)
		}
	}
	if e = s.check(); e != nil {
		return result, e
	}
	if intent.Result.Descriptor.Evidence != s.evidence || intent.SourceEvidence != s.sourceEvidence {
		return result, ErrBindingMismatch
	}
	digest, e := checkBindingCheckpoint(ctx, o.Export, o.Request.ExpectedSchema, result.Generation, s.r.identity.Hash)
	if e != nil || digest != result.ManifestSHA256 {
		return result, errors.Join(e, ErrPackage)
	}
	// Existing controls must be one of the exact old/new states covered by this
	// intent. A matching SQL row never permits overwriting arbitrary material.
	var storedDescriptor BindingDescriptor
	if x := readBindingFile(ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &storedDescriptor); x == nil {
		if storedDescriptor != result.Descriptor && (result.Prior == nil || storedDescriptor != *result.Prior) {
			return result, ErrBindingMismatch
		}
	} else if !os.IsNotExist(x) {
		return result, x
	}
	var storedMarker BindingMarker
	if x := readBindingFile(ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &storedMarker); x == nil {
		if storedMarker != result.Descriptor.Marker && (result.Prior == nil || storedMarker != result.Prior.Marker) {
			return result, ErrBindingMismatch
		}
	} else if !os.IsNotExist(x) {
		return result, x
	}
	// Only a matching durable intent and actual committed catalog authorize repair.
	for _, x := range []struct {
		d     *bindingDirectory
		name  string
		max   int
		value any
	}{{s.control, bindingMarkerName, BindingMarkerMaxBytes, result.Descriptor.Marker}, {s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, result.Descriptor}} {
		if e = removeBindingStaging(ctx, x.d, x.name, x.max, x.value); e != nil {
			return result, e
		}
		if e = publishBindingFile(ctx, x.d, x.name, x.value, x.max, s.check, h); e != nil {
			return result, e
		}
		if e = h.at("published:"+x.name, nil); e != nil {
			return result, e
		}
	}
	var d BindingDescriptor
	var m BindingMarker
	if e = errors.Join(readBindingFile(ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &d), readBindingFile(ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &m)); e != nil {
		return result, e
	}
	if d != result.Descriptor || m != d.Marker {
		return result, ErrBindingMismatch
	}
	if e = h.at("before-intent-removal", nil); e != nil {
		return result, e
	}
	if e = s.check(); e != nil {
		return result, e
	}
	if e = s.installation.root.Remove(bindingIntentName); e != nil {
		return result, e
	}
	if e = s.installation.file.Sync(); e != nil {
		return result, e
	}
	result.Status = "complete; bounded maintenance only"
	return result, s.check()
}
func matchingBindingIntent(i bindingIntent, o BindingOptions) bool {
	checkpoint, e := EncodeBindingPath(o.Export.Destination)
	return e == nil && i.Kind == "coldkeep-binding-intent/v1" && i.Request == o.Request && i.PlanSHA256 == o.PlanSHA256 && i.CheckpointBase64 == checkpoint && i.Result.PlanSHA256 == o.PlanSHA256 && i.Result.Descriptor.Marker.OperationID == o.Request.OperationID && validDescriptor(i.Result.Descriptor)
}
func checkBindingCheckpoint(ctx context.Context, o ExportOptions, version int, generation, rootHash string) (string, error) {
	root, e := os.OpenRoot(o.Destination)
	if e != nil {
		return "", e
	}
	m, raw, e := checkGeneration(ctx, root, o.Limits, true)
	e = errors.Join(e, root.Close())
	if e != nil {
		return "", e
	}
	if m.CatalogVersion != version || m.Generation != generation || m.LegacyRootHash != rootHash {
		return "", ErrPackage
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
