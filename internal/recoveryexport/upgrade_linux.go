package recoveryexport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

type UpgradeRequest struct {
	InstallationDirectory string `json:"installation_directory"`
	ExpectedSchema        int    `json:"expected_schema"`
	RepositoryID          string `json:"repository_id"`
	RootID                string `json:"root_id"`
	ExpectedRevision      int64  `json:"expected_revision"`
	OperationID           string `json:"operation_id"`
}
type UpgradeOptions struct {
	Request    UpgradeRequest `json:"request"`
	PlanSHA256 string         `json:"plan_sha256"`
	Export     ExportOptions  `json:"export"`
}
type UpgradeResult struct {
	Status              string            `json:"status"`
	OperationID         string            `json:"operation_id"`
	PlanSHA256          string            `json:"plan_sha256"`
	Generation          string            `json:"generation"`
	ManifestSHA256      string            `json:"manifest_sha256"`
	Descriptor          BindingDescriptor `json:"descriptor"`
	OrdinaryWriterReady bool              `json:"ordinary_writer_ready"`
}
type upgradeIntent struct {
	CatalogSHA256    string         `json:"catalog_sha256"`
	Kind             string         `json:"kind"`
	Request          UpgradeRequest `json:"request"`
	PlanSHA256       string         `json:"plan_sha256"`
	PriorPlan        string         `json:"prior_plan"`
	CheckpointBase64 string         `json:"checkpoint_base64"`
	Result           UpgradeResult  `json:"result"`
}

func validUpgradeRequest(q UpgradeRequest) bool {
	return q.ExpectedSchema == 19 && q.ExpectedRevision > 0 && q.ExpectedRevision < 20000 && db.ValidIdentity("repo", q.RepositoryID) && db.ValidIdentity("root", q.RootID) && db.ValidIdentity("upgrade", q.OperationID)
}
func (s *bindingSession) upgradeDescriptor(q UpgradeRequest, i *Inventory) (BindingDescriptor, error) {
	if e := errors.Join(bindingFileAbsent(s.installation, bindingDescriptorName+".pending"), bindingFileAbsent(s.control, bindingMarkerName+".pending")); e != nil {
		return BindingDescriptor{}, e
	}
	d, e := currentBindingDescriptor(i, bindingLocator(s.c))
	if e != nil {
		return d, e
	}
	if d.Marker.RepositoryID != q.RepositoryID || d.Marker.RootID != q.RootID || d.Marker.Revision != q.ExpectedRevision {
		return d, ErrBindingMismatch
	}
	var stored BindingDescriptor
	var marker BindingMarker
	if e = errors.Join(readBindingFile(s.ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &stored), readBindingFile(s.ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &marker)); e != nil {
		return d, e
	}
	path, e := EncodeBindingPath(s.root.path)
	if e != nil || !validDescriptor(d) || stored != d || marker != d.Marker || d.RootPathBase64 != path || d.Evidence != s.evidence {
		return d, ErrBindingMismatch
	}
	for _, r := range i.Catalog["source_binding_revision"] {
		path, e := decodeBindingPath(r.s("path_base64"))
		if e != nil {
			return d, e
		}
		if pathContains(path, s.installation.path) || pathContains(s.installation.path, path) {
			return d, ErrConfiguration
		}
	}
	return d, s.check()
}
func (s *bindingSession) upgradePreview(q UpgradeRequest, i *Inventory) (UpgradeResult, error) {
	result := UpgradeResult{Status: "preview; ordinary writers refused", OperationID: q.OperationID}
	if i.CatalogVersion != 19 {
		return result, ErrBindingMismatch
	}
	d, e := s.upgradeDescriptor(q, i)
	if e != nil {
		return result, e
	}
	result.Descriptor = d
	result.PlanSHA256 = bindingDigest(struct {
		Request    UpgradeRequest
		Catalog    string
		Descriptor BindingDescriptor
	}{q, adoptionPlan(s.c, i), d})
	return result, nil
}
func PreviewUpgrade(ctx context.Context, c Config, q UpgradeRequest, l Limits) (result UpgradeResult, err error) {
	if ctx == nil || !validUpgradeRequest(q) || !l.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, l.Duration)
	defer cancel()
	s, e := openMaintenanceSession(ctx, c, q.InstallationDirectory, "", "", nil)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil {
			result.Status = "preview refused"
		}
	}()
	if e = errors.Join(bindingFileAbsent(s.installation, bindingIntentName), bindingFileAbsent(s.installation, bindingIntentName+".pending")); e != nil {
		return result, errors.Join(ErrBindingReconciliation, e)
	}
	i, e := s.inventory(l)
	if e != nil {
		return result, e
	}
	return s.upgradePreview(q, i)
}
func ApplyUpgrade(ctx context.Context, c Config, o UpgradeOptions) (UpgradeResult, error) {
	return applyUpgrade(ctx, c, o, nil)
}
func ReconcileUpgrade(ctx context.Context, c Config, o UpgradeOptions) (UpgradeResult, error) {
	return applyUpgrade(ctx, c, o, nil)
}
func applyUpgrade(ctx context.Context, c Config, o UpgradeOptions, h *bindingHooks) (result UpgradeResult, err error) {
	result.Status = "incomplete; schema upgrade not committed"
	if ctx == nil || !validUpgradeRequest(o.Request) || !o.Export.Limits.valid() || !o.Export.InventoryLimits.valid() || !safeDigest.MatchString(o.PlanSHA256) {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, min(o.Export.Limits.Duration, o.Export.InventoryLimits.Duration))
	defer cancel()
	s, e := openMaintenanceSession(ctx, c, o.Request.InstallationDirectory, "", o.Export.Destination, h)
	if e != nil {
		return result, e
	}
	committed := false
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil {
			result.Status = "incomplete; reconciliation required"
			if committed {
				err = errors.Join(err, ErrBindingReconciliation)
			}
		}
	}()
	i, e := s.inventory(o.Export.InventoryLimits)
	if e != nil {
		return result, e
	}
	d, e := s.upgradeDescriptor(o.Request, i)
	if e != nil {
		return result, e
	}
	intent, present, staged, e := readUpgradeIntent(ctx, s)
	if e != nil {
		return result, e
	}
	if present && !matchingUpgradeIntent(intent, o, d) {
		return result, ErrBindingMismatch
	}
	switch i.CatalogVersion {
	case 20:
		committed = true
		op := i.Catalog["schema_upgrade_operation"][0]
		if op.s("operation_id") != o.Request.OperationID || op.s("request_sha256") != bindingDigest(o.Request) || op.s("plan_sha256") != o.PlanSHA256 {
			return result, ErrBindingMismatch
		}
		result = UpgradeResult{Status: "complete; recovery-only schema20", OperationID: o.Request.OperationID, PlanSHA256: o.PlanSHA256, Generation: op.s("generation"), ManifestSHA256: op.s("manifest_sha256"), Descriptor: d}
		if present && (intent.PriorPlan != op.s("prior_plan") || intent.Result.Generation != result.Generation || intent.Result.ManifestSHA256 != result.ManifestSHA256) {
			return result, ErrBindingMismatch
		}
	case 19:
		preview, e := s.upgradePreview(o.Request, i)
		if e != nil || preview.PlanSHA256 != o.PlanSHA256 {
			return result, errors.Join(e, ErrBindingMismatch)
		}
		if present {
			if intent.PriorPlan != adoptionPlan(s.c, i) || intent.CatalogSHA256 != upgradeCatalogDigest(i) {
				return result, ErrBindingMismatch
			}
			preview.Generation = intent.Result.Generation
			preview.ManifestSHA256 = intent.Result.ManifestSHA256
			if bindingDigest(preview) != bindingDigest(intent.Result) {
				return result, ErrBindingMismatch
			}
			result = intent.Result
		} else {
			result = preview
			if e = h.at("before-checkpoint", nil); e != nil {
				return result, e
			}
			owned := ownedCut{s.r.identity, s.lease}
			var eh *exportHooks
			if h != nil {
				eh = h.export
			}
			checkpoint, e := exportOwned(ctx, s.c, o.Export, eh, &s.r, &owned)
			if e != nil || !checkpoint.Complete {
				return result, errors.Join(e, ErrPackage)
			}
			result.Generation = checkpoint.Generation
			result.ManifestSHA256, e = checkBindingCheckpoint(ctx, o.Export, 19, result.Generation, s.r.identity.Hash)
			if e != nil {
				return result, e
			}
			encoded, e := EncodeBindingPath(o.Export.Destination)
			if e != nil {
				return result, e
			}
			intent = upgradeIntent{Kind: "coldkeep-schema-upgrade-intent/v1", CatalogSHA256: upgradeCatalogDigest(i), Request: o.Request, PlanSHA256: o.PlanSHA256, PriorPlan: adoptionPlan(s.c, i), CheckpointBase64: encoded, Result: result}
			if e = h.at("before-intent", nil); e != nil {
				return result, e
			}
			if e = publishBindingFile(ctx, s.installation, bindingIntentName, intent, BindingIntentMaxBytes, s.check, h); e != nil {
				return result, e
			}
			present = true
			if e = h.at("after-intent", nil); e != nil {
				return result, e
			}
		}
		if e = verifyUpgradeCheckpoint(ctx, o, result, s.r.identity.Hash); e != nil {
			return result, e
		}
		fresh, e := s.inventory(o.Export.InventoryLimits)
		if e != nil {
			return result, e
		}
		if adoptionPlan(s.c, fresh) != intent.PriorPlan {
			return result, ErrBindingMismatch
		}
		// Promote only a completely validated, exact staged intent. Partial/foreign
		// staging is retained for explicit diagnosis, never overwritten or guessed.
		if staged {
			if e = s.check(); e != nil {
				return result, e
			}
			if e = s.installation.root.Rename(bindingIntentName+".pending", bindingIntentName); e != nil {
				return result, e
			}
			if e = s.installation.file.Sync(); e != nil {
				return result, e
			}
			staged = false
		}
		committed, e = s.commitUpgrade(o, intent)
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
		actual, e := s.upgradeDescriptor(o.Request, i)
		if e != nil || actual != d || i.CatalogVersion != 20 {
			return result, errors.Join(e, ErrBindingMismatch)
		}
	default:
		return result, ErrUnsupported
	}
	if e = verifyUpgradeCheckpoint(ctx, o, result, s.r.identity.Hash); e != nil {
		return result, e
	}
	if e = h.at("before-intent-removal", nil); e != nil {
		return result, e
	}
	if e = s.check(); e != nil {
		return result, e
	}
	if present {
		name := bindingIntentName
		if staged {
			name += ".pending"
		}
		if e = s.installation.root.Remove(name); e != nil {
			return result, e
		}
		if e = s.installation.file.Sync(); e != nil {
			return result, e
		}
	}
	result.Status = "complete; recovery-only schema20"
	return result, s.check()
}
func readUpgradeIntent(ctx context.Context, s *bindingSession) (intent upgradeIntent, present, staged bool, err error) {
	e := readBindingFile(ctx, s.installation, bindingIntentName, BindingIntentMaxBytes, &intent)
	if e == nil {
		if x := bindingFileAbsent(s.installation, bindingIntentName+".pending"); x != nil {
			return intent, true, false, x
		}
		return intent, true, false, nil
	}
	if !os.IsNotExist(e) {
		return intent, false, false, e
	}
	e = readBindingFile(ctx, s.installation, bindingIntentName+".pending", BindingIntentMaxBytes, &intent)
	if os.IsNotExist(e) {
		return intent, false, false, nil
	}
	return intent, e == nil, true, e
}
func matchingUpgradeIntent(i upgradeIntent, o UpgradeOptions, d BindingDescriptor) bool {
	path, e := EncodeBindingPath(o.Export.Destination)
	return e == nil && i.Kind == "coldkeep-schema-upgrade-intent/v1" && i.Request == o.Request && i.PlanSHA256 == o.PlanSHA256 && i.CheckpointBase64 == path && i.Result.OperationID == o.Request.OperationID && i.Result.PlanSHA256 == o.PlanSHA256 && i.Result.Descriptor == d && !i.Result.OrdinaryWriterReady && safeDigest.MatchString(i.CatalogSHA256) && safeDigest.MatchString(i.PriorPlan) && safeDigest.MatchString(i.Result.ManifestSHA256) && safeGeneration.MatchString(i.Result.Generation)
}
func verifyUpgradeCheckpoint(ctx context.Context, o UpgradeOptions, r UpgradeResult, rootHash string) error {
	root, e := os.OpenRoot(o.Export.Destination)
	if e != nil {
		return e
	}
	m, raw, e := checkGeneration(ctx, root, o.Export.Limits, true)
	e = errors.Join(e, root.Close())
	if e != nil {
		return e
	}
	sum := sha256.Sum256(raw)
	if m.CatalogVersion != 19 || m.RepositoryID != o.Request.RepositoryID || m.Generation != r.Generation || m.LegacyRootHash != rootHash || hex.EncodeToString(sum[:]) != r.ManifestSHA256 {
		return ErrPackage
	}
	return nil
}

func upgradeCatalogDigest(i *Inventory) string {
	return adoptionPlan(Config{}, &Inventory{Catalog: i.Catalog})
}
