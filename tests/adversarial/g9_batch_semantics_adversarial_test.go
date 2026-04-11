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

func storeAdversarialBatchFileWithPath(t *testing.T, repoRoot, binPath string, env map[string]string, inputDir, name, content string) (int64, string) {
	t.Helper()
	path := filepath.Join(inputDir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write input file %s: %v", name, err)
	}
	store := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"store", path, "--output", "json"), "store")
	data := testutils.JSONMap(t, store, "data")
	fileID := testutils.JSONInt64(t, data, "file_id")
	storedPath, _ := data["stored_path"].(string)
	if strings.TrimSpace(storedPath) == "" {
		t.Fatalf("store payload missing stored_path: %v", store)
	}
	return fileID, storedPath
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
			if msg, _ := dryItem["message"].(string); strings.TrimSpace(msg) == "" {
				t.Fatalf("dry-run item should include message at index %d: %v", i, dryItem)
			}
			if msg, _ := realItem["message"].(string); strings.TrimSpace(msg) == "" {
				t.Fatalf("real item should include message at index %d: %v", i, realItem)
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
		results, ok := payload["results"].([]any)
		if !ok || len(results) != 5 {
			t.Fatalf("unexpected duplicate explosion results shape: %v", payload)
		}
		seenSuccessMessage := false
		skippedWithMessage := 0
		for _, raw := range results {
			item, _ := raw.(map[string]any)
			status, _ := item["status"].(string)
			msg, _ := item["message"].(string)
			switch status {
			case "success":
				if strings.Contains(strings.ToLower(msg), "removed mappings=") {
					seenSuccessMessage = true
				}
			case "skipped":
				if strings.Contains(strings.ToLower(msg), "duplicate target") {
					skippedWithMessage++
				}
			}
		}
		if !seenSuccessMessage {
			t.Fatalf("duplicate explosion should include success message detail: %v", payload)
		}
		if skippedWithMessage != 4 {
			t.Fatalf("duplicate explosion should include duplicate-target skipped messages for all duplicates: %v", payload)
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

	t.Run("stored-path fail-fast preserves ordering and isolation", func(t *testing.T) {
		id1, storedPath1 := storeAdversarialBatchFileWithPath(t, repoRoot, binPath, env, inputDir, "g9-sp-fast-a.txt", "g9-sp-fast-a")
		id2, storedPath2 := storeAdversarialBatchFileWithPath(t, repoRoot, binPath, env, inputDir, "g9-sp-fast-b.txt", "g9-sp-fast-b")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "--stored-paths", storedPath1, "/missing/stored/path", storedPath2, "--fail-fast", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("stored-path fail-fast should return non-zero\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}

		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 1 || summaryCount(t, payload, "failed") != 1 {
			t.Fatalf("unexpected stored-path fail-fast summary: %v", payload)
		}

		results, ok := payload["results"].([]any)
		if !ok || len(results) != 2 {
			t.Fatalf("expected fail-fast to stop after first failure, results=%v payload=%v", results, payload)
		}
		first, _ := results[0].(map[string]any)
		second, _ := results[1].(map[string]any)
		if first["status"] != "success" {
			t.Fatalf("expected first stored-path item success, got %v", first)
		}
		if second["status"] != "failed" {
			t.Fatalf("expected second stored-path item failure, got %v", second)
		}
		if raw, _ := first["raw_value"].(string); raw != storedPath1 {
			t.Fatalf("expected first raw_value=%q, got %v", storedPath1, first)
		}
		if raw, _ := second["raw_value"].(string); raw != "/missing/stored/path" {
			t.Fatalf("expected second raw_value for missing path, got %v", second)
		}

		outDir := filepath.Join(tmp, "g9-stored-path-fast-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreSucceeds(t, repoRoot, binPath, env, id1, outDir)
		expectRestoreSucceeds(t, repoRoot, binPath, env, id2, outDir)
	})

	t.Run("stored-path duplicate explosion remove", func(t *testing.T) {
		id, storedPath := storeAdversarialBatchFileWithPath(t, repoRoot, binPath, env, inputDir, "g9-sp-dup.txt", "g9-sp-dup")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "--stored-paths", storedPath, storedPath, storedPath, storedPath, storedPath, "--output", "json")
		if res.ExitCode != 0 {
			t.Fatalf("stored-path duplicate explosion should succeed\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}
		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 1 || summaryCount(t, payload, "skipped") != 4 || summaryCount(t, payload, "failed") != 0 {
			t.Fatalf("unexpected stored-path duplicate summary: %v", payload)
		}

		results, ok := payload["results"].([]any)
		if !ok || len(results) != 5 {
			t.Fatalf("unexpected stored-path duplicate results shape: %v", payload)
		}
		skippedWithMessage := 0
		for _, raw := range results {
			item, _ := raw.(map[string]any)
			status, _ := item["status"].(string)
			msg, _ := item["message"].(string)
			rawValue, _ := item["raw_value"].(string)
			if rawValue != storedPath {
				t.Fatalf("stored-path duplicate item should preserve raw_value=%q, got %v", storedPath, item)
			}
			if status == "skipped" && strings.Contains(strings.ToLower(msg), "duplicate target") {
				skippedWithMessage++
			}
		}
		if skippedWithMessage != 4 {
			t.Fatalf("expected duplicate-target skipped message for all duplicate stored paths: %v", payload)
		}

		outDir := filepath.Join(tmp, "g9-sp-dup-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreSucceeds(t, repoRoot, binPath, env, id, outDir)
	})

	t.Run("stored-path mixed input ordering under dry-run", func(t *testing.T) {
		id1, storedPath1 := storeAdversarialBatchFileWithPath(t, repoRoot, binPath, env, inputDir, "g9-sp-order-a.txt", "g9-sp-order-a")
		id2, storedPath2 := storeAdversarialBatchFileWithPath(t, repoRoot, binPath, env, inputDir, "g9-sp-order-b.txt", "g9-sp-order-b")

		inputFile := filepath.Join(inputDir, "g9_sp_order_paths.txt")
		content := fmt.Sprintf("# stored path batch\n%s\n%s\n", storedPath2, storedPath1)
		if err := os.WriteFile(inputFile, []byte(content), 0o644); err != nil {
			t.Fatalf("write stored-path input file: %v", err)
		}

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "--stored-paths", storedPath1, "/missing/stored/path/order", "--input", inputFile, "--dry-run", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected non-zero because stored-path input includes invalid path\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}

		payload := parseBatchPayload(t, res)
		results, ok := payload["results"].([]any)
		if !ok || len(results) != 4 {
			t.Fatalf("unexpected stored-path mixed-order results payload: %v", payload)
		}

		first, _ := results[0].(map[string]any)
		second, _ := results[1].(map[string]any)
		third, _ := results[2].(map[string]any)
		fourth, _ := results[3].(map[string]any)

		if first["status"] != "planned" || first["raw_value"] != storedPath1 {
			t.Fatalf("unexpected first stored-path result: %v", first)
		}
		if second["status"] != "failed" || second["raw_value"] != "/missing/stored/path/order" {
			t.Fatalf("unexpected second stored-path result: %v", second)
		}
		if third["status"] != "planned" || third["raw_value"] != storedPath2 {
			t.Fatalf("unexpected third stored-path result: %v", third)
		}
		if fourth["status"] != "skipped" || fourth["raw_value"] != storedPath1 {
			t.Fatalf("unexpected fourth stored-path result: %v", fourth)
		}
		if msg, _ := fourth["message"].(string); !strings.Contains(strings.ToLower(msg), "duplicate target") {
			t.Fatalf("expected duplicate-target skip message on fourth stored-path result: %v", fourth)
		}

		outDir := filepath.Join(tmp, "g9-sp-order-restore")
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		expectRestoreSucceeds(t, repoRoot, binPath, env, id1, outDir)
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

	t.Run("ordered reporting under mixed input", func(t *testing.T) {
		id1 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-order-a.txt", "g9-order-a")
		id2 := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-order-b.txt", "g9-order-b")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", fmt.Sprintf("%d", id1), "abc", fmt.Sprintf("%d", id2), fmt.Sprintf("%d", id1), "--dry-run", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected non-zero because mixed input includes invalid token\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}

		payload := parseBatchPayload(t, res)
		results, ok := payload["results"].([]any)
		if !ok || len(results) != 4 {
			t.Fatalf("unexpected mixed-order results payload: %v", payload)
		}

		first, _ := results[0].(map[string]any)
		second, _ := results[1].(map[string]any)
		third, _ := results[2].(map[string]any)
		fourth, _ := results[3].(map[string]any)

		if first["status"] != "planned" || int64(first["id"].(float64)) != id1 {
			t.Fatalf("unexpected first result for mixed order: %v", first)
		}
		if second["status"] != "failed" {
			t.Fatalf("unexpected second result for mixed order: %v", second)
		}
		if third["status"] != "planned" || int64(third["id"].(float64)) != id2 {
			t.Fatalf("unexpected third result for mixed order: %v", third)
		}
		if fourth["status"] != "skipped" || int64(fourth["id"].(float64)) != id1 {
			t.Fatalf("unexpected fourth result for mixed order: %v", fourth)
		}
		if msg, _ := first["message"].(string); strings.TrimSpace(msg) == "" {
			t.Fatalf("planned first result should include message: %v", first)
		}
		if msg, _ := third["message"].(string); strings.TrimSpace(msg) == "" {
			t.Fatalf("planned third result should include message: %v", third)
		}
		if msg, _ := fourth["message"].(string); !strings.Contains(strings.ToLower(msg), "duplicate target") {
			t.Fatalf("skipped fourth result should include duplicate-target message: %v", fourth)
		}
	})

	t.Run("all-invalid input returns structured failed items", func(t *testing.T) {
		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "abc", "def", "ghi", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("all-invalid remove input should fail\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}

		payload := parseBatchPayload(t, res)
		if summaryCount(t, payload, "success") != 0 || summaryCount(t, payload, "failed") != 3 || summaryCount(t, payload, "skipped") != 0 {
			t.Fatalf("unexpected all-invalid summary: %v", payload)
		}

		results, ok := payload["results"].([]any)
		if !ok || len(results) != 3 {
			t.Fatalf("unexpected all-invalid results shape: %v", payload)
		}
		for _, raw := range results {
			item, _ := raw.(map[string]any)
			if status, _ := item["status"].(string); status != "failed" {
				t.Fatalf("all-invalid item should be failed: %v", item)
			}
			if rawValue, _ := item["raw_value"].(string); strings.TrimSpace(rawValue) == "" {
				t.Fatalf("all-invalid item should include raw_value: %v", item)
			}
			if _, hasID := item["id"]; hasID {
				t.Fatalf("all-invalid item should not include id: %v", item)
			}
			if errText, _ := item["error"].(string); !strings.Contains(strings.ToLower(errText), "invalid file id") {
				t.Fatalf("all-invalid item should include invalid-file-id error detail: %v", item)
			}
		}
	})

	t.Run("invalid input serializes as raw_value without id zero", func(t *testing.T) {
		id := storeAdversarialBatchFile(t, repoRoot, binPath, env, inputDir, "g9-raw-value.txt", "g9-raw-value")

		res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "abc", fmt.Sprintf("%d", id), "--dry-run", "--output", "json")
		if res.ExitCode == 0 {
			t.Fatalf("expected non-zero because invalid token is included\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
		}

		payload := parseBatchPayload(t, res)
		results, ok := payload["results"].([]any)
		if !ok || len(results) < 1 {
			t.Fatalf("unexpected results payload: %v", payload)
		}

		invalid, _ := results[0].(map[string]any)
		if status, _ := invalid["status"].(string); status != "failed" {
			t.Fatalf("expected first result to be failed invalid token, got=%v", invalid)
		}
		if raw, _ := invalid["raw_value"].(string); raw != "abc" {
			t.Fatalf("expected raw_value=abc for invalid token, got=%v", invalid)
		}
		if _, hasID := invalid["id"]; hasID {
			t.Fatalf("invalid token result must not serialize id field (no id=0): %v", invalid)
		}
	})
}
