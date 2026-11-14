package recoveryexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

var bindingTables = []tableSpec{
	{"binding_operation", "operation_id plan_sha256 request_sha256 root_id action from_revision to_revision generation manifest_sha256", "operation_id plan_sha256 request_sha256 root_id action generation manifest_sha256", "text text text text text int8 int8 text text", "operation_id plan_sha256 request_sha256 root_id action from_revision to_revision generation manifest_sha256"},
	{"root_binding_revision", "root_id revision installation_id incarnation path_base64 evidence operation_id", "root_id installation_id incarnation path_base64 evidence operation_id", "text int8 text text text text text", "root_id revision installation_id incarnation path_base64 evidence operation_id"},
	{"root_binding_head", "root_id revision capability", "root_id capability", "text int8 text", "root_id revision capability"},
	{"configured_source", "source_id repository_id slot capture_enabled disabled", "source_id repository_id", "text text int4 bool bool", "source_id repository_id slot capture_enabled disabled"},
	{"source_binding_revision", "source_id revision path_base64 evidence continuity fresh_scan operation_id", "source_id path_base64 evidence continuity operation_id", "text int8 text text text bool text", "source_id revision path_base64 evidence continuity fresh_scan operation_id"},
	{"source_binding_head", "source_id revision", "source_id", "text int8", "source_id revision"},
}

func validateBindingConstraints(ctx context.Context, tx *sql.Tx) error {
	all := make(map[identityConstraintKey]identityConstraintSpec, len(identityConstraints)+len(bindingConstraints))
	for k, v := range identityConstraints {
		all[k] = v
	}
	for k, v := range bindingConstraints {
		all[k] = v
	}
	names := []string{}
	for _, t := range append(append([]tableSpec(nil), identityTables...), bindingTables...) {
		names = append(names, "'"+t.name+"'")
	}
	relations := ` FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND t.relname IN (` + strings.Join(names, ",") + `)`
	return validateExactConstraints(ctx, tx, all, relations)
}
func bindingRowEvidence(r Row, source bool) (BindingEvidence, error) {
	var e BindingEvidence
	if err := decodeBindingRecord(context.Background(), []byte(r.s("evidence")), 4096, &e); err != nil || !validEvidence(e, source) {
		return e, graphError("binding evidence")
	}
	return e, nil
}
func validateBindingGraph(i *Inventory) error {
	if i.CatalogVersion != 19 && i.CatalogVersion != 20 {
		return nil
	}
	bad := func() error { return graphError("binding lineage") }
	repo := i.Catalog["repository_identity"][0].s("repository_id")
	root := i.Catalog["storage_root_identity"][0].s("root_id")
	heads := i.Catalog["root_binding_head"]
	if len(heads) != 1 || heads[0].s("root_id") != root || heads[0].s("capability") != bindingCapability {
		return bad()
	}
	head := heads[0].n("revision")
	revisions := i.Catalog["root_binding_revision"]
	operations := i.Catalog["binding_operation"]
	if head < 1 || head != int64(len(revisions)) || len(operations) != len(revisions) {
		return bad()
	}
	ops, err := strIndex(operations, "operation_id")
	if err != nil {
		return err
	}
	revs := map[int64]Row{}
	seenOps := map[string]bool{}
	for _, r := range revisions {
		n := r.n("revision")
		op := ops[r.s("operation_id")]
		if n < 1 || n > head || revs[n] != nil || op == nil || seenOps[r.s("operation_id")] || r.s("root_id") != root || op.s("root_id") != root || op.n("from_revision") != n-1 || op.n("to_revision") != n || !validBindingID("bind", op.s("operation_id")) || !safeDigest.MatchString(op.s("plan_sha256")) || !safeDigest.MatchString(op.s("request_sha256")) || !safeDigest.MatchString(op.s("manifest_sha256")) || !safeGeneration.MatchString(op.s("generation")) || !validBindingID("install", r.s("installation_id")) || !validBindingID("incarnation", r.s("incarnation")) {
			return bad()
		}
		if _, e := decodeBindingPath(r.s("path_base64")); e != nil {
			return e
		}
		if _, e := bindingRowEvidence(r, false); e != nil {
			return e
		}
		revs[n] = r
		seenOps[r.s("operation_id")] = true
	}
	for n := int64(1); n <= head; n++ {
		r := revs[n]
		op := ops[r.s("operation_id")]
		action := BindingAction(op.s("action"))
		if n == 1 {
			if action != BindingInitial {
				return bad()
			}
			continue
		}
		old := revs[n-1]
		if r.s("installation_id") != old.s("installation_id") {
			return bad()
		}
		switch action {
		case BindingRelocate:
			if r.s("incarnation") != old.s("incarnation") || r.s("evidence") != old.s("evidence") || r.s("path_base64") == old.s("path_base64") {
				return bad()
			}
		case BindingReconnect:
			a, _ := bindingRowEvidence(old, false)
			b, _ := bindingRowEvidence(r, false)
			if r.s("incarnation") == old.s("incarnation") || !sameBindingObjects(a, b) {
				return bad()
			}
		case BindingSourceRegister, BindingSourceRebind, BindingSourceDisable:
			if r.s("incarnation") != old.s("incarnation") || r.s("evidence") != old.s("evidence") || r.s("path_base64") != old.s("path_base64") {
				return bad()
			}
		default:
			return bad()
		}
	}
	sources, e := strIndex(i.Catalog["configured_source"], "source_id")
	if e != nil {
		return e
	}
	sh, e := strIndex(i.Catalog["source_binding_head"], "source_id")
	if e != nil {
		return e
	}
	if len(sources) > BindingMaxSources || len(sh) != len(sources) {
		return bad()
	}
	sr := map[string]map[int64]Row{}
	slots := map[int64]bool{}
	sourceOps := map[string]bool{}
	for id, r := range sources {
		slot := r.n("slot")
		if !db.ValidIdentity("source", id) || id == i.Catalog["source_identity"][0].s("source_id") || r.s("repository_id") != repo || r["capture_enabled"] != false || slot < 1 || slot > BindingMaxSources || slots[slot] || sh[id] == nil {
			return bad()
		}
		slots[slot] = true
		sr[id] = map[int64]Row{}
	}
	for _, r := range i.Catalog["source_binding_revision"] {
		id := r.s("source_id")
		n := r.n("revision")
		op := ops[r.s("operation_id")]
		if sr[id] == nil || n < 1 || sr[id][n] != nil || op == nil || sourceOps[r.s("operation_id")] {
			return bad()
		}
		if _, e := decodeBindingPath(r.s("path_base64")); e != nil {
			return e
		}
		if _, e := bindingRowEvidence(r, true); e != nil {
			return e
		}
		sourceOps[r.s("operation_id")] = true
		sr[id][n] = r
	}
	for id, rows := range sr {
		if int64(len(rows)) != sh[id].n("revision") || len(rows) < 1 {
			return bad()
		}
		for n := int64(1); n <= int64(len(rows)); n++ {
			r := rows[n]
			op := ops[r.s("operation_id")]
			if op == nil {
				return bad()
			}
			action := BindingAction(op.s("action"))
			cont := r.s("continuity")
			if n == 1 {
				if action != BindingSourceRegister || cont != "new" || !r.b("fresh_scan") {
					return bad()
				}
				continue
			}
			old := rows[n-1]
			prior := ops[old.s("operation_id")]
			if op.n("to_revision") <= prior.n("to_revision") || old.s("continuity") == "disabled" {
				return bad()
			}
			switch action {
			case BindingSourceRebind:
				if cont != "same-object" && cont != "changed" {
					return bad()
				}
				same := old.s("evidence") == r.s("evidence")
				if (cont == "same-object") != same || r.b("fresh_scan") == same {
					return bad()
				}
			case BindingSourceDisable:
				if cont != "disabled" || r.s("path_base64") != old.s("path_base64") || r.s("evidence") != old.s("evidence") || !r.b("fresh_scan") {
					return bad()
				}
			default:
				return bad()
			}
		}
		if sources[id].b("disabled") != (rows[int64(len(rows))].s("continuity") == "disabled") {
			return bad()
		}
	}
	for id, op := range ops {
		isSource := strings.HasPrefix(op.s("action"), "source-")
		if isSource != sourceOps[id] {
			return bad()
		}
	}
	return nil
}
func currentBindingDescriptor(i *Inventory, locator BindingLocator) (BindingDescriptor, error) {
	if i.CatalogVersion != 19 && i.CatalogVersion != 20 {
		return BindingDescriptor{}, ErrUnsupported
	}
	return bindingDescriptorAt(i, locator, i.Catalog["root_binding_head"][0].n("revision"))
}
func bindingDescriptorAt(i *Inventory, locator BindingLocator, head int64) (BindingDescriptor, error) {
	for _, r := range i.Catalog["root_binding_revision"] {
		if r.n("revision") == head {
			for _, op := range i.Catalog["binding_operation"] {
				if op.s("operation_id") == r.s("operation_id") {
					evidence, e := bindingRowEvidence(r, false)
					if e != nil {
						return BindingDescriptor{}, e
					}
					return BindingDescriptor{"coldkeep-installation/v1", BindingMarker{"coldkeep-root-marker/v1", i.Catalog["repository_identity"][0].s("repository_id"), r.s("root_id"), r.s("installation_id"), r.s("incarnation"), head, r.s("operation_id"), op.s("plan_sha256")}, locator, r.s("path_base64"), evidence}, nil
				}
			}
		}
	}
	return BindingDescriptor{}, fmt.Errorf("%w: missing binding head", ErrCorrupt)
}
func bindingJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
