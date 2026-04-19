package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func verifySystemMustFailWithInvariantG10(t *testing.T, repoRoot, binPath string, env map[string]string) map[string]any {
	t.Helper()

	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "verify", "system", "--output", "json")
	if res.ExitCode == 0 {
		t.Fatalf("verify should fail on drifted graph\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	errPayload, ok := testutils.FindCLIErrorPayload(res.Stderr)
	if !ok {
		errPayload, ok = testutils.FindCLIErrorPayload(res.Stdout + "\n" + res.Stderr)
	}
	if !ok {
		t.Fatalf("verify failure should include machine-readable error payload\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	if code, _ := errPayload["invariant_code"].(string); strings.TrimSpace(code) == "" {
		t.Fatalf("verify failure payload missing invariant_code: %v", errPayload)
	}
	return errPayload
}

func TestAdversarialG10PhysicalGraphIntegrityDetectedAndRepaired(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG2Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG2Codec(t, codec)
			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g10-anchor.bin", 350*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, codec, anchorPath)

			if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 3 WHERE id = $1`, anchorID); err != nil {
				t.Fatalf("inject logical_file.ref_count drift: %v", err)
			}

			errPayload := verifySystemMustFailWithInvariantG10(t, repoRoot, binPath, env)
			if got, _ := errPayload["invariant_code"].(string); got != "PHYSICAL_GRAPH_REFCOUNT_MISMATCH" {
				t.Fatalf("expected invariant_code PHYSICAL_GRAPH_REFCOUNT_MISMATCH, got %q payload=%v", got, errPayload)
			}

			repair := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
				"repair", "ref-counts", "--output", "json"), "repair")
			repairData := testutils.JSONMap(t, repair, "data")
			if updated := testutils.JSONInt64(t, repairData, "updated_logical_files"); updated < 1 {
				t.Fatalf("expected repair updated_logical_files >= 1, got %d payload=%v", updated, repair)
			}

			testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
				"verify", "system", "--output", "json"), "verify")
			restoreMustMatchHashG2(t, dbconn, anchorID, filepath.Join(restoreDir, "g10-anchor.restored.bin"), anchorHash)
		})
	}
}

func TestAdversarialG11GCRefusesOnDriftedGraphAndSucceedsAfterRepair(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}

	anchorPath := testutils.CreateTempFile(t, inputDir, "g11-anchor.bin", 300*1024)
	anchorHash := testutils.SHA256File(t, anchorPath)
	anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, "plain", anchorPath)

	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 2 WHERE id = $1`, anchorID); err != nil {
		t.Fatalf("inject logical_file.ref_count drift: %v", err)
	}

	gcRefused := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "gc", "--output", "json")
	if gcRefused.ExitCode == 0 {
		t.Fatalf("gc should be refused on drifted graph\nstdout:\n%s\nstderr:\n%s", gcRefused.Stdout, gcRefused.Stderr)
	}
	gcErr, ok := testutils.FindCLIErrorPayload(gcRefused.Stderr)
	if !ok {
		gcErr, ok = testutils.FindCLIErrorPayload(gcRefused.Stdout + "\n" + gcRefused.Stderr)
	}
	if !ok {
		t.Fatalf("gc refusal should include machine-readable error payload\nstdout:\n%s\nstderr:\n%s", gcRefused.Stdout, gcRefused.Stderr)
	}
	if got, _ := gcErr["invariant_code"].(string); got != "GC_REFUSED_INTEGRITY" {
		t.Fatalf("expected invariant_code GC_REFUSED_INTEGRITY, got %q payload=%v", got, gcErr)
	}

	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"repair", "ref-counts", "--output", "json"), "repair")
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"gc", "--dry-run", "--output", "json"), "gc")
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"gc", "--output", "json"), "gc")
	restoreMustMatchHashG2(t, dbconn, anchorID, filepath.Join(restoreDir, "g11-anchor.restored.bin"), anchorHash)
}

func TestAdversarialG12InvariantCodeStabilityUnderConcurrentInjection(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG2Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	anchorPath := testutils.CreateTempFile(t, inputDir, "g12-anchor.bin", 260*1024)
	anchorID := storeFileWithCodecCLIG2(t, repoRoot, binPath, env, "plain", anchorPath)

	concurrentPath := filepath.Join(inputDir, "g12-concurrent.bin")
	if err := os.WriteFile(concurrentPath, []byte(strings.Repeat("g12-concurrent", 4096)), 0o644); err != nil {
		t.Fatalf("write concurrent input: %v", err)
	}

	var storeErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				storeErr = fmt.Errorf("panic: %v", r)
			}
		}()
		_ = storeFileWithCodecCLIG2(t, repoRoot, binPath, env, "plain", concurrentPath)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_, _ = dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = $1`, anchorID)
		}
	}()
	wg.Wait()

	if storeErr != nil {
		t.Fatalf("concurrent store failed: %v", storeErr)
	}

	errPayload1 := verifySystemMustFailWithInvariantG10(t, repoRoot, binPath, env)
	errPayload2 := verifySystemMustFailWithInvariantG10(t, repoRoot, binPath, env)
	code1, _ := errPayload1["invariant_code"].(string)
	code2, _ := errPayload2["invariant_code"].(string)
	if strings.TrimSpace(code1) == "" || strings.TrimSpace(code2) == "" {
		t.Fatalf("verify must always return invariant_code on broken state: payload1=%v payload2=%v", errPayload1, errPayload2)
	}
}

func TestAdversarialG13BatchRepairReportIsDeterministicUnderPartialFailure(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, _ := setupAdversarialG2Env(t)
	defer dbconn.Close()

	run := func() map[string]any {
		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"repair", "ref-counts", "unknown-target", "ref-counts", "--batch", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected non-zero batch repair because one target is invalid\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		return parseBatchPayload(t, res)
	}

	payload1 := run()
	payload2 := run()

	for idx, payload := range []map[string]any{payload1, payload2} {
		if summaryCount(t, payload, "success") != 1 || summaryCount(t, payload, "failed") != 1 || summaryCount(t, payload, "skipped") != 1 {
			t.Fatalf("run %d unexpected batch summary: %v", idx+1, payload)
		}
		results, ok := payload["results"].([]any)
		if !ok || len(results) != 3 {
			t.Fatalf("run %d unexpected batch results shape: %v", idx+1, payload)
		}
		for _, raw := range results {
			item, _ := raw.(map[string]any)
			if status, _ := item["status"].(string); strings.TrimSpace(status) == "" {
				t.Fatalf("run %d result missing status: %v", idx+1, item)
			}
			if rawValue, _ := item["raw_value"].(string); strings.TrimSpace(rawValue) == "" {
				t.Fatalf("run %d result missing raw_value: %v", idx+1, item)
			}
			msg, _ := item["message"].(string)
			errText, _ := item["error"].(string)
			if strings.TrimSpace(msg) == "" && strings.TrimSpace(errText) == "" {
				t.Fatalf("run %d result should include message or error: %v", idx+1, item)
			}
		}
	}

	results1, _ := payload1["results"].([]any)
	results2, _ := payload2["results"].([]any)
	if len(results1) != len(results2) {
		t.Fatalf("batch repair result length drift between runs: run1=%d run2=%d", len(results1), len(results2))
	}
	for i := range results1 {
		i1, _ := results1[i].(map[string]any)
		i2, _ := results2[i].(map[string]any)
		if i1["status"] != i2["status"] || i1["raw_value"] != i2["raw_value"] {
			t.Fatalf("batch repair determinism mismatch at index %d: run1=%v run2=%v", i, i1, i2)
		}
	}
}
