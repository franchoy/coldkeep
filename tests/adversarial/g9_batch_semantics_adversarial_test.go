package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func parseBatchPayload(t *testing.T, res testutils.CLIExecResult) map[string]any {
	t.Helper()

	payload, ok := testutils.TryParseLastJSONLine(res.Stdout)
	if !ok {
		payload, ok = testutils.TryParseLastJSONLine(res.Stdout + "\n" + res.Stderr)
	}
	if !ok {
		t.Fatalf("batch command produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	return payload
}

func summaryCount(t *testing.T, payload map[string]any, key string) int {
	t.Helper()
	summary := testutils.JSONMap(t, payload, "summary")
	return int(testutils.JSONInt64(t, summary, key))
}

func storeAdversarialBatchFile(t *testing.T, repoRoot, binPath string, env map[string]string, inputDir, name, content string) int64 {
	t.Helper()
	path := filepath.Join(inputDir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write input file %s: %v", name, err)
	}
	store := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"store", path, "--output", "json"), "store")
	return testutils.JSONInt64(t, testutils.JSONMap(t, store, "data"), "file_id")
}

func expectRestoreFails(t *testing.T, repoRoot, binPath string, env map[string]string, fileID int64, outDir string) {
	t.Helper()
	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"restore", fmt.Sprintf("%d", fileID), outDir, "--output", "json")
	if res.ExitCode == 0 {
		t.Fatalf("expected restore failure for fileID=%d, got success\nstdout:\n%s\nstderr:\n%s", fileID, res.Stdout, res.Stderr)
	}
}

func expectRestoreSucceeds(t *testing.T, repoRoot, binPath string, env map[string]string, fileID int64, outDir string) {
	t.Helper()
	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"restore", fmt.Sprintf("%d", fileID), outDir, "--overwrite", "--output", "json")
	if res.ExitCode != 0 {
		t.Fatalf("expected restore success for fileID=%d\nstdout:\n%s\nstderr:\n%s", fileID, res.Stdout, res.Stderr)
	}
}

func TestAdversarialG9BatchSemanticsOrchestration(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input dir: %v", err)
	}

	t.Run("partial failure resilience remove", func(t *testing.T) {
		id1 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-partial-a.txt", "g9-partial-a")
		id2 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-partial-b.txt", "g9-partial-b")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", fmt.Sprintf("%d", id1), "999999", fmt.Sprintf("%d", id2), "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected non-zero due to mixed valid/invalid IDs\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 2 || summaryCount(t, payload, "failed") != 1 {
			t.Fatalf("unexpected summary for partial failure resilience: %v", payload)
		}

		outDir := filepath.Join(tmp, "g9-partial-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreFails(t, repoRoot, binPath, env, id1, outDir)
		expectRestoreFails(t, repoRoot, binPath, env, id2, outDir)
	})

	t.Run("dry-run parity restore", func(t *testing.T) {
		id1 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-parity-a.txt", "g9-parity-a")
		id2 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-parity-b.txt", "g9-parity-b")

		outDir := filepath.Join(tmp, "g9-parity-out")
		dryRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"restore", fmt.Sprintf("%d", id1), fmt.Sprintf("%d", id2), outDir, "--dry-run", "--output", "json")
		if dryRes.ExitCode != 0 {
			t.Fatalf("dry-run restore failed\nstdout:\n%s\nstderr:\n%s", dryRes.Stdout, dryRes.Stderr)
		}
		dryPayload := parseBatchPayload(t, dryRes)
		if summaryCount(t, dryPayload, "planned") != 2 || summaryCount(t, dryPayload, "failed") != 0 {
			t.Fatalf("unexpected dry-run summary: %v", dryPayload)
		}

		realRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"restore", fmt.Sprintf("%d", id1), fmt.Sprintf("%d", id2), outDir, "--overwrite", "--output", "json")
		if realRes.ExitCode != 0 {
			t.Fatalf("real restore failed\nstdout:\n%s\nstderr:\n%s", realRes.Stdout, realRes.Stderr)
		}
		realPayload := parseBatchPayload(t, realRes)
		if summaryCount(t, realPayload, "success") != 2 || summaryCount(t, realPayload, "failed") != 0 {
			t.Fatalf("unexpected real summary: %v", realPayload)
		}

		dryResults, ok := dryPayload["results"].([]any)
		if !ok || len(dryResults) != 2 {
			t.Fatalf("unexpected dry-run results shape: %v", dryPayload)
		}
		realResults, ok := realPayload["results"].([]any)
		if !ok || len(realResults) != 2 {
			t.Fatalf("unexpected real results shape: %v", realPayload)
		}

		for i := 0; i < 2; i++ {
			dryItem, _ := dryResults[i].(map[string]any)
			realItem, _ := realResults[i].(map[string]any)
			if dryItem["id"] != realItem["id"] {
				t.Fatalf("dry-run and real item id mismatch at index %d: dry=%v real=%v", i, dryItem, realItem)
			}
			if dryItem["output_path"] != realItem["output_path"] {
				t.Fatalf("dry-run and real output path mismatch at index %d: dry=%v real=%v", i, dryItem, realItem)
			}
		}
	})

	t.Run("duplicate explosion remove", func(t *testing.T) {
		id := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-dup.txt", "g9-dup")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", fmt.Sprintf("%d", id), fmt.Sprintf("%d", id), fmt.Sprintf("%d", id), fmt.Sprintf("%d", id), fmt.Sprintf("%d", id), "--output", "json")
		if res.ExitCode != 0 {
			t.Fatalf("duplicate explosion remove should succeed\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 1 || summaryCount(t, payload, "skipped") != 4 || summaryCount(t, payload, "failed") != 0 {
			t.Fatalf("unexpected duplicate explosion summary: %v", payload)
		}

		outDir := filepath.Join(tmp, "g9-dup-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreFails(t, repoRoot, binPath, env, id, outDir)
	})

	t.Run("fail-fast correctness", func(t *testing.T) {
		id1 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-fast-a.txt", "g9-fast-a")
		id2 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-fast-b.txt", "g9-fast-b")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", fmt.Sprintf("%d", id1), "999999", fmt.Sprintf("%d", id2), "--fail-fast", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected fail-fast run to return non-zero\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 1 || summaryCount(t, payload, "failed") != 1 {
			t.Fatalf("unexpected fail-fast summary: %v", payload)
		}

		outDir := filepath.Join(tmp, "g9-fail-fast-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreFails(t, repoRoot, binPath, env, id1, outDir)
		expectRestoreSucceeds(t, repoRoot, binPath, env, id2, outDir)
	})

	t.Run("input file mixed chaos", func(t *testing.T) {
		id1 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-input-a.txt", "g9-input-a")
		id2 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-input-b.txt", "g9-input-b")

		idsPath := filepath.Join(inputDir, "g9_ids.txt")
		content := fmt.Sprintf("# test\n%d\n\nabc\n%d\n%d\n", id1, id2, id1)
		if err := os.WriteFile(idsPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write ids file: %v", err)
		}

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "--input", idsPath, "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("mixed input chaos should be partial failure due to invalid entry\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 2 || summaryCount(t, payload, "failed") != 1 || summaryCount(t, payload, "skipped") != 1 {
			t.Fatalf("unexpected mixed input summary: %v", payload)
		}

		results, ok := payload["results"].([]any)
		if !ok || len(results) < 4 {
			t.Fatalf("unexpected mixed input results shape: %v", payload)
		}
		foundInvalid := false
		for _, raw := range results {
			item, _ := raw.(map[string]any)
			if status, _ := item["status"].(string); status == "failed" {
				if errText, _ := item["error"].(string); strings.Contains(strings.ToLower(errText), "invalid file id") {
					foundInvalid = true
				}
			}
		}
		if !foundInvalid {
			t.Fatalf("mixed input chaos should include invalid-file-id failed item: %v", payload)
		}
	})
}
