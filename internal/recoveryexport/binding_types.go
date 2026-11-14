package recoveryexport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	BindingMarkerMaxBytes     = 16 << 10
	BindingDescriptorMaxBytes = 64 << 10
	BindingIntentMaxBytes     = 128 << 10
	BindingPathMaxBytes       = 3072
	BindingMaxSources         = 64
	bindingReader             = "Coldkeep schema-19 physical-binding-v1/recovery-only, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM"
	bindingCapability         = "physical-binding-v1/recovery-only"
	bindingEvidenceClass      = "linux-local-unique-mount-session-v1"
	bindingMarkerName         = "root-marker.v1.json"
	bindingDescriptorName     = "installation.v1.json"
	bindingIntentName         = "binding-intent.v1.json"
)

var (
	ErrBindingMismatch       = errors.New("binding tuple or reviewed request mismatch")
	ErrBindingReconciliation = errors.New("binding reconciliation required; retain intent and checkpoint")
	ErrBindingUnavailable    = errors.New("binding root or source unavailable")
	bindingIDPattern         = regexp.MustCompile(`^(bind|install|incarnation)_[0-9a-f]{32}$`)
)

type BindingAction string

const (
	BindingInitial        BindingAction = "bind"
	BindingRelocate       BindingAction = "relocate"
	BindingReconnect      BindingAction = "rebind"
	BindingSourceRegister BindingAction = "source-register"
	BindingSourceRebind   BindingAction = "source-rebind"
	BindingSourceDisable  BindingAction = "source-disable"
)

// BindingRequest contains only explicit selections. Exact native path bytes are
// represented by base64 inside persisted records; display text is never authority.
type BindingRequest struct {
	Action                 BindingAction `json:"action"`
	InstallationDirectory  string        `json:"installation_directory"`
	ExpectedSchema         int           `json:"expected_schema"`
	ExpectedRevision       int64         `json:"expected_revision"`
	OperationID            string        `json:"operation_id"`
	SourceID               string        `json:"source_id"`
	SourcePathBase64       string        `json:"source_path_base64"`
	ExpectedSourceRevision int64         `json:"expected_source_revision"`
}
type BindingObjectEvidence struct {
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	Mount      uint64 `json:"mount_unique_id"`
	Filesystem int64  `json:"filesystem"`
}
type BindingEvidence struct {
	Class     string                `json:"class"`
	Boot      string                `json:"boot"`
	Namespace uint64                `json:"namespace"`
	Root      BindingObjectEvidence `json:"root"`
	Control   BindingObjectEvidence `json:"control"`
	Lock      BindingObjectEvidence `json:"lock"`
}
type BindingMarker struct {
	Kind           string `json:"kind"`
	RepositoryID   string `json:"repository_id"`
	RootID         string `json:"root_id"`
	InstallationID string `json:"installation_id"`
	Incarnation    string `json:"incarnation"`
	Revision       int64  `json:"revision"`
	OperationID    string `json:"operation_id"`
	PlanSHA256     string `json:"plan_sha256"`
}
type BindingLocator struct {
	Host         string `json:"host"`
	Port         uint16 `json:"port"`
	Database     string `json:"database"`
	User         string `json:"user"`
	Profile      string `json:"profile"`
	KeyReference string `json:"key_reference"`
}
type BindingDescriptor struct {
	Kind           string          `json:"kind"`
	Marker         BindingMarker   `json:"marker"`
	Locator        BindingLocator  `json:"locator"`
	RootPathBase64 string          `json:"root_path_base64"`
	Evidence       BindingEvidence `json:"evidence"`
}
type BindingResult struct {
	Status              string             `json:"status"`
	PlanSHA256          string             `json:"plan_sha256"`
	Descriptor          BindingDescriptor  `json:"descriptor"`
	Prior               *BindingDescriptor `json:"prior,omitempty"`
	SourceID            string             `json:"source_id,omitempty"`
	SourceRevision      int64              `json:"source_revision,omitempty"`
	Continuity          string             `json:"continuity,omitempty"`
	Generation          string             `json:"generation,omitempty"`
	ManifestSHA256      string             `json:"manifest_sha256,omitempty"`
	OrdinaryWriterReady bool               `json:"ordinary_writer_ready"`
}
type bindingIntent struct {
	Kind             string          `json:"kind"`
	Request          BindingRequest  `json:"request"`
	PlanSHA256       string          `json:"plan_sha256"`
	PriorPlan        string          `json:"prior_catalog_sha256"`
	Result           BindingResult   `json:"result"`
	SourceEvidence   BindingEvidence `json:"source_evidence"`
	CheckpointBase64 string          `json:"checkpoint_base64"`
}

func bindingID(kind string) (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return kind + "_" + hex.EncodeToString(b[:]), nil
}
func validBindingID(kind, s string) bool {
	return strings.HasPrefix(s, kind+"_") && bindingIDPattern.MatchString(s)
}
func bindingDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func bindingLocator(c Config) BindingLocator {
	return BindingLocator{c.Host, c.Port, c.Database, c.User, c.Profile, c.KeyReference}
}

// EncodeBindingPath rejects ambiguous syntax; it does not alter native bytes.
func EncodeBindingPath(path string) (string, error) {
	if len(path) == 0 || len(path) > BindingPathMaxBytes || strings.ContainsRune(path, 0) || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return "", ErrConfiguration
	}
	return base64.StdEncoding.EncodeToString([]byte(path)), nil
}
func decodeBindingPath(encoded string) (string, error) {
	if len(encoded) > 4096 {
		return "", ErrLimit
	}
	b, e := base64.StdEncoding.Strict().DecodeString(encoded)
	if e != nil {
		return "", ErrConfiguration
	}
	s := string(b)
	again, e := EncodeBindingPath(s)
	if e != nil || again != encoded {
		return "", ErrConfiguration
	}
	return s, nil
}
func decodeBindingRecord(ctx context.Context, b []byte, max int, v any) error {
	if len(b) > max {
		return ErrLimit
	}
	return strictJSON(ctx, b, v, BindingMaxSources)
}
func validEvidence(e BindingEvidence, source bool) bool {
	valid := func(o BindingObjectEvidence) bool {
		return o.Inode > 0 && o.Mount > 0 && (o.Filesystem == 0xef53 || o.Filesystem == 0x58465342 || o.Filesystem == 0x01021994 || o.Filesystem == 0x794c7630)
	}
	if e.Class != bindingEvidenceClass || len(e.Boot) != 36 || e.Namespace == 0 || !valid(e.Root) {
		return false
	}
	if source {
		return e.Control == (BindingObjectEvidence{}) && e.Lock == (BindingObjectEvidence{})
	}
	return valid(e.Control) && valid(e.Lock)
}
func sameBindingObjects(a, b BindingEvidence) bool {
	same := func(x, y BindingObjectEvidence) bool {
		return x.Device == y.Device && x.Inode == y.Inode && x.Filesystem == y.Filesystem
	}
	return same(a.Root, b.Root) && same(a.Control, b.Control) && same(a.Lock, b.Lock)
}
func validDescriptor(d BindingDescriptor) bool {
	m := d.Marker
	_, e := decodeBindingPath(d.RootPathBase64)
	return e == nil && d.Kind == "coldkeep-installation/v1" && m.Kind == "coldkeep-root-marker/v1" && validBindingID("install", m.InstallationID) && validBindingID("incarnation", m.Incarnation) && validBindingID("bind", m.OperationID) && safeDigest.MatchString(m.PlanSHA256) && m.Revision > 0 && validEvidence(d.Evidence, false) && d.Locator.Profile == "manual-postgresql-docker"
}

// DecodeBindingPath returns the exact validated native bytes from an explicit
// base64 path. It never uses lossy display text to select a root.
func DecodeBindingPath(encoded string) (string, error) { return decodeBindingPath(encoded) }
