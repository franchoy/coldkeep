package recoveryexport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"golang.org/x/sys/unix"
)

func r1Reader(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("COLDKEEP_RECOVERY_READER")
	if binary == "" {
		t.Fatal("ordinary runner must stage the actual recovery operator")
	}
	return binary
}

func r1MoveRoot(t *testing.T, f *fixture, path string) {
	t.Helper()
	must(t, os.Rename(f.root, path))
	f.root, f.config.PayloadRoot = path, path
}

// Each sibling has its own normally prepared catalog, control directory and
// persistent lock. Neither a missing path nor a broken fixture can explain
// which physical object the operator selects.
func r1RootPair(t *testing.T) (*fixture, BindingOptions, *fixture, BindingOptions) {
	t.Helper()
	f, o := bindingFixture(t)
	r1MoveRoot(t, f, filepath.Join(filepath.Dir(f.root), " Root-� "))
	native, no := bindingFixture(t)
	r1MoveRoot(t, native, filepath.Join(filepath.Dir(f.root), " Root-"+string([]byte{0xff})+" "))
	a, e := os.Stat(f.root)
	must(t, e)
	b, e := os.Stat(native.root)
	must(t, e)
	if os.SameFile(a, b) || bytes.Equal([]byte(f.root), []byte(native.root)) {
		t.Fatal("replacement-character and native-byte fixtures collapsed")
	}
	return f, o, native, no
}

// Only the selected JSON string representation changes; credentials and all
// other fields remain the normally staged, valid operator configuration.
func r1Config(t *testing.T, d Destination, encoding string) (string, []byte, []string) {
	t.Helper()
	path := readerConfig(t, d)
	raw, e := os.ReadFile(path)
	must(t, e)
	var values map[string]json.RawMessage
	must(t, json.Unmarshal(raw, &values))
	var credentials []string
	for _, field := range []string{"password_file", "key_file"} {
		var value string
		must(t, json.Unmarshal(values[field], &value))
		if value != "" {
			credentials = append(credentials, value)
		}
	}
	if encoding == "base64-native-byte" {
		delete(values, "payload_root")
		values["payload_root_base64"], e = json.Marshal(base64.StdEncoding.EncodeToString([]byte(d.PayloadRoot)))
		must(t, e)
	} else {
		values["payload_root"], e = json.Marshal(d.PayloadRoot)
		must(t, e)
	}
	raw, e = json.Marshal(values)
	must(t, e)
	switch encoding {
	case "raw-invalid-utf8":
		raw = bytes.ReplaceAll(raw, []byte("�"), []byte{0xff})
	case "lone-low-surrogate":
		raw = bytes.ReplaceAll(raw, []byte("�"), []byte(`\udcff`))
	case "lone-high-surrogate":
		raw = bytes.ReplaceAll(raw, []byte("�"), []byte(`\ud800`))
	case "escaped-replacement-rune":
		raw = bytes.ReplaceAll(raw, []byte("�"), []byte(`\ufffd`))
	case "paired-surrogates":
		raw = bytes.ReplaceAll(raw, []byte("😀"), []byte(`\ud83d\ude00`))
	}
	must(t, os.WriteFile(path, raw, 0600))
	return path, raw, credentials
}

func r1BindingArgs(command, config string, o BindingOptions) []string {
	args := []string{command, "-config", config, "-installation-directory", o.Request.InstallationDirectory,
		"-binding-action", string(o.Request.Action), "-expected-schema", strconv.Itoa(o.Request.ExpectedSchema),
		"-expected-revision", strconv.FormatInt(o.Request.ExpectedRevision, 10), "-operation-id", o.Request.OperationID}
	if command == "binding-apply" {
		args = append(args, "-plan-sha256", o.PlanSHA256, "-checkpoint", o.Export.Destination)
	}
	return args
}

func r1Preview(t *testing.T, path string, o BindingOptions, want string) BindingResult {
	t.Helper()
	out, e := runReader(t, r1Reader(t), r1BindingArgs("binding-preview", path, o)...)
	if e != nil {
		t.Fatalf("valid actual operator preview: %v %s", e, out)
	}
	var result BindingResult
	must(t, json.Unmarshal(out, &result))
	selected, e := DecodeBindingPath(result.Descriptor.RootPathBase64)
	must(t, e)
	if !bytes.Equal([]byte(selected), []byte(want)) || result.PlanSHA256 == "" || result.OrdinaryWriterReady {
		t.Fatalf("operator selected different bytes: got=%x want=%x result=%+v", []byte(selected), []byte(want), result)
	}
	return result
}

func r1Record(t *testing.T, record map[string]any) {
	t.Helper()
	raw, e := json.MarshalIndent(record, "", "  ")
	must(t, e)
	t.Logf("R1 evidence %s", raw)
	if dir := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); dir != "" {
		name := "r1-" + strings.ReplaceAll(t.Name(), "/", "-") + ".json"
		must(t, os.WriteFile(filepath.Join(dir, name), append(raw, '\n'), 0600))
	}
}

// Watching actual credential opens/accesses makes a path-refusal diagnostic
// insufficient on its own: these normally valid files must never be consumed.
func r1WatchCredentialUse(t *testing.T, paths []string) func() {
	t.Helper()
	fd, e := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	must(t, e)
	t.Cleanup(func() { must(t, unix.Close(fd)) })
	for _, path := range paths {
		_, e = unix.InotifyAddWatch(fd, path, unix.IN_OPEN|unix.IN_ACCESS)
		must(t, e)
	}
	return func() {
		t.Helper()
		b := make([]byte, 65536)
		n, e := unix.Read(fd, b)
		if n > 0 || !errors.Is(e, unix.EAGAIN) {
			t.Fatalf("credential use before config refusal: bytes=%d error=%v", n, e)
		}
	}
}

func r1Preserved(t *testing.T, f, sibling *fixture, o BindingOptions) func() {
	t.Helper()
	roots := []string{f.root, sibling.root, o.Request.InstallationDirectory, filepath.Dir(o.Export.Destination)}
	fingerprints := make([]string, len(roots))
	watches := make([]func() bool, len(roots))
	for n, path := range roots {
		fingerprints[n] = treeFingerprint(t, path)
		watches[n] = watchMutations(t, path)
	}
	catalog, siblingCatalog := catalogFingerprint(t, f.db), catalogFingerprint(t, sibling.db)
	lockPath := filepath.Join(f.root, ".coldkeep-control", "repository.lock")
	lock, e := os.Stat(lockPath)
	must(t, e)
	identity, e := coordination.ResolveIdentity(f.root)
	must(t, e)
	return func() {
		t.Helper()
		for n, changed := range watches {
			if changed() || fingerprints[n] != treeFingerprint(t, roots[n]) {
				t.Fatalf("transient or lasting filesystem mutation in %x", []byte(roots[n]))
			}
		}
		if catalog != catalogFingerprint(t, f.db) || siblingCatalog != catalogFingerprint(t, sibling.db) {
			t.Fatal("refused operator changed a logical catalog")
		}
		after, e := os.Stat(lockPath)
		must(t, e)
		now, e := coordination.ResolveIdentity(f.root)
		must(t, e)
		if !os.SameFile(lock, after) || identity != now {
			t.Fatal("persistent lock or selected-root identity changed")
		}
		for _, path := range []string{o.Export.Destination,
			filepath.Join(f.root, ".coldkeep-control", bindingMarkerName),
			filepath.Join(o.Request.InstallationDirectory, bindingDescriptorName),
			filepath.Join(o.Request.InstallationDirectory, bindingIntentName),
			filepath.Join(o.Request.InstallationDirectory, bindingIntentName+".pending")} {
			if _, e = os.Lstat(path); !os.IsNotExist(e) {
				t.Fatalf("refused operator created checkpoint or binding state: %s: %v", path, e)
			}
		}
		assertBindingNoSessions(t, f)
		assertBindingNoSessions(t, sibling)
	}
}

func TestBindingOperatorConfigFidelity(t *testing.T) {
	f, o, native, no := r1RootPair(t)
	valid, _, _ := r1Config(t, Destination(f.config), "literal-replacement-rune")
	baseline := r1Preview(t, valid, o, f.root)
	o.PlanSHA256 = baseline.PlanSHA256
	nativeConfig, _, _ := r1Config(t, Destination(native.config), "base64-native-byte")
	nativePreview := r1Preview(t, nativeConfig, no, native.root)
	if baseline.Descriptor.Evidence.Root == nativePreview.Descriptor.Evidence.Root {
		t.Fatal("distinct native roots have equal physical evidence")
	}
	for _, encoding := range []string{"raw-invalid-utf8", "lone-low-surrogate", "lone-high-surrogate"} {
		t.Run(encoding, func(t *testing.T) {
			path, raw, credentials := r1Config(t, Destination(f.config), encoding)
			for _, command := range []string{"preview", "apply"} {
				t.Run(command, func(t *testing.T) {
					preserved := r1Preserved(t, f, native, o)
					unused := r1WatchCredentialUse(t, credentials)
					out, e := runReader(t, r1Reader(t), r1BindingArgs("binding-"+command, path, o)...)
					unused()
					preserved()
					if e == nil || !strings.Contains(string(out), "invalid configuration Unicode") {
						t.Fatalf("not an early Unicode admission refusal: %v %s", e, out)
					}
					r1Record(t, map[string]any{"input_hex": hex.EncodeToString(raw), "positive_control_root_base64": baseline.Descriptor.RootPathBase64,
						"native_sibling_base64": nativePreview.Descriptor.RootPathBase64, "valid_plan_sha256": o.PlanSHA256,
						"request": o.Request, "refusal": string(out), "catalog_unchanged": true, "trees_unchanged": true,
						"lock_identity_unchanged": true, "filesystem_write_events": 0, "credential_access_events": 0, "owned_sessions": 0})
				})
			}
		})
	}
	// The exact request and plan supplied to every negative apply remain usable.
	r1ApplyAndCheck(t, valid, f, o, baseline)
	r1ApplyAndCheck(t, nativeConfig, native, no, nativePreview)
}

func r1ApplyAndCheck(t *testing.T, config string, f *fixture, o BindingOptions, preview BindingResult) {
	t.Helper()
	o.PlanSHA256 = preview.PlanSHA256
	lockPath := filepath.Join(f.root, ".coldkeep-control", "repository.lock")
	lock, e := os.Stat(lockPath)
	must(t, e)
	legacy := legacyRowsFingerprint(t, f.db)
	out, e := runReader(t, r1Reader(t), r1BindingArgs("binding-apply", config, o)...)
	if e != nil {
		t.Fatalf("valid actual operator apply: %v %s", e, out)
	}
	var applied BindingResult
	must(t, json.Unmarshal(out, &applied))
	if applied.Descriptor.RootPathBase64 != preview.Descriptor.RootPathBase64 || applied.Descriptor.Evidence != preview.Descriptor.Evidence ||
		applied.Descriptor.Marker.RepositoryID != preview.Descriptor.Marker.RepositoryID || applied.Descriptor.Marker.RootID != preview.Descriptor.Marker.RootID ||
		applied.OrdinaryWriterReady || applied.Status != "complete; bounded maintenance only" || legacy != legacyRowsFingerprint(t, f.db) {
		t.Fatal("valid native path changed binding or historical identity", applied)
	}
	var stored string
	must(t, f.db.QueryRow("SELECT path_base64 FROM root_binding_revision WHERE revision=1").Scan(&stored))
	if stored != applied.Descriptor.RootPathBase64 {
		t.Fatal("SQL did not preserve exact path bytes")
	}
	out, e = runReader(t, r1Reader(t), "binding-check", "-config", config, "-installation-directory", o.Request.InstallationDirectory)
	if e != nil {
		t.Fatalf("valid actual operator binding check: %v %s", e, out)
	}
	var status BindingResult
	must(t, json.Unmarshal(out, &status))
	if status.Descriptor != applied.Descriptor || status.OrdinaryWriterReady {
		t.Fatal("binding check changed selected identity", status)
	}
	after, e := os.Stat(lockPath)
	must(t, e)
	if !os.SameFile(lock, after) {
		t.Fatal("supported binding replaced persistent lock")
	}
	assertBindingNoSessions(t, f)
}

func TestBindingOperatorNativePathControls(t *testing.T) {
	for _, test := range []struct{ encoding, name string }{
		{"literal-replacement-rune", " Root-� "}, {"escaped-replacement-rune", " Root-� "},
		{"paired-surrogates", " Root-😀 "}, {"literal-backslash-u", ` Root-\udcff `},
		{"whitespace-case-nonascii", " Root-Ä "}, {"base64-native-byte", " Root-" + string([]byte{0xff}) + " "},
	} {
		t.Run(test.encoding, func(t *testing.T) {
			f, o := bindingFixture(t)
			r1MoveRoot(t, f, filepath.Join(filepath.Dir(f.root), test.name))
			path, raw, _ := r1Config(t, Destination(f.config), test.encoding)
			preview := r1Preview(t, path, o, f.root)
			if test.encoding == "escaped-replacement-rune" {
				literal, _, _ := r1Config(t, Destination(f.config), "literal-replacement-rune")
				if other := r1Preview(t, literal, o, f.root); other != preview {
					t.Fatal("literal and escaped U+FFFD selected different identities")
				}
			}
			r1ApplyAndCheck(t, path, f, o, preview)
			r1Record(t, map[string]any{"input_hex": hex.EncodeToString(raw), "expected_root_hex": hex.EncodeToString([]byte(f.root)),
				"preview": preview, "preview_apply_check": "PASS", "identity_continuity": "PASS", "owned_sessions": 0})
		})
	}
}

func TestBindingOperatorHistoricalRestore(t *testing.T) {
	f, files := recoveryHistory(t, false)
	_ = bindHistory(t, f)
	pkg := exportOptions(t)
	_, e := Export(context.Background(), f.config, pkg)
	must(t, e)
	d, open := importTarget(t, f)
	d.PayloadRoot = filepath.Join(filepath.Dir(d.PayloadRoot), " Historical-� ")
	opts := importOptionsFor(t, pkg)
	config, _, _ := r1Config(t, d, "escaped-replacement-rune")
	out, e := runReader(t, r1Reader(t), "import", "-config", config, "-package", pkg.Destination,
		"-generation", opts.Generation, "-manifest-sha256", opts.ManifestSHA256, "-trusted-operator-backup",
		"-exclusive-target", "-database-capacity-bytes", "805306368")
	if e != nil {
		t.Fatalf("actual operator historical import: %v %s", e, out)
	}
	output := filepath.Join(t.TempDir(), "restored")
	out, e = runReader(t, r1Reader(t), "restore", "-config", config, "-snapshot", "independent-history", "-output", output)
	if e != nil {
		t.Fatalf("actual operator historical restore: %v %s", e, out)
	}
	checkRestoredHistory(t, output, files)
	conn := open()
	var pins, sessions int
	must(t, conn.QueryRow("SELECT COALESCE(sum(pin_count),0) FROM chunk").Scan(&pins))
	must(t, conn.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name LIKE 'coldkeep-%'").Scan(&sessions))
	if pins != 0 || sessions != 0 {
		t.Fatal("historical operator left pins or sessions", pins, sessions)
	}
	r1Record(t, map[string]any{"root_base64": base64.StdEncoding.EncodeToString([]byte(d.PayloadRoot)), "files": files,
		"actual_staged_import_restore": "PASS", "reread_hashes_metadata": "PASS", "pins": pins, "owned_sessions": sessions})
}

func TestBindingOperatorMutationObserver(t *testing.T) {
	root := t.TempDir()
	changed := watchMutations(t, root)
	path := filepath.Join(root, "removed-before-observation")
	must(t, os.WriteFile(path, []byte("transient"), 0600))
	must(t, os.Remove(path))
	if !changed() {
		t.Fatal("observer missed create-write-remove")
	}
	credential := filepath.Join(root, "credential")
	must(t, os.WriteFile(credential, []byte("observer-only"), 0600))
	fd, e := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	must(t, e)
	defer func() { must(t, unix.Close(fd)) }()
	_, e = unix.InotifyAddWatch(fd, credential, unix.IN_OPEN|unix.IN_ACCESS)
	must(t, e)
	_, e = os.ReadFile(credential)
	must(t, e)
	b := make([]byte, 4096)
	n, e := unix.Read(fd, b)
	must(t, e)
	if n <= 0 {
		t.Fatal("observer missed credential read")
	}
}
