package benchmark

import (
	"path/filepath"
	"strings"
	"testing"
)

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix)
		}
	}
	return ""
}

func TestCoreScenariosReturnsExpectedNames(t *testing.T) {
	scenarios := CoreScenarios(ScenarioConfig{})
	if len(scenarios) != 9 {
		t.Fatalf("expected 9 scenarios, got %d", len(scenarios))
	}

	want := []string{
		"store-large-file",
		"store-many-small-files",
		"store-mixed-dataset",
		"restore-large-file",
		"restore-many-files",
		"snapshot-creation",
		"gc-after-churn",
		"stats-inspect",
		"verify-system-deep",
	}
	for i, name := range want {
		if scenarios[i].Name != name {
			t.Fatalf("scenario[%d] mismatch: got=%q want=%q", i, scenarios[i].Name, name)
		}
	}
}

func TestScenarioStoreLargeFileExecutesStoreCommand(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:             runner.run,
		ColdkeepExecutable: "coldkeep",
		LargeFileSizeBytes: 1024,
	}
	scenario := scenarioByName(t, CoreScenarios(cfg), "store-large-file")

	metrics, err := Measure(func() error {
		return scenario.Run(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()})
	})
	if err != nil {
		t.Fatalf("scenario run failed: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected 1 command call, got %d", len(runner.calls))
	}
	call := runner.calls[0]
	if call.Executable != "coldkeep" {
		t.Fatalf("executable mismatch: got=%q", call.Executable)
	}
	if len(call.Args) < 1 || call.Args[0] != "store" {
		t.Fatalf("expected store command, got=%v", call.Args)
	}
	if metrics.FilesProcessed != 1 || metrics.BytesProcessed != 1024 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestScenarioGCAfterChurnRunsExpectedFlow(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:                 runner.run,
		ColdkeepExecutable:     "coldkeep",
		ManySmallFileCount:     6,
		ManySmallFileSizeBytes: 128,
		RemoveEvery:            2,
	}
	scenario := scenarioByName(t, CoreScenarios(cfg), "gc-after-churn")

	repoPath := t.TempDir()
	dataPath := t.TempDir()
	if err := scenario.Run(BenchmarkContext{RepoPath: repoPath, DataPath: dataPath}); err != nil {
		t.Fatalf("scenario run failed: %v", err)
	}

	if len(runner.calls) < 4 {
		t.Fatalf("expected at least 4 command calls, got %d", len(runner.calls))
	}
	if runner.calls[0].Args[0] != "store-folder" {
		t.Fatalf("expected first command store-folder, got %v", runner.calls[0].Args)
	}

	joined := runner.joinedCommands()
	if !containsCommand(joined, "snapshot create churn/ --id bench-snapshot-gc") {
		t.Fatalf("expected snapshot create command, got=%v", joined)
	}
	for _, call := range runner.calls {
		if len(call.Args) >= 2 && call.Args[0] == "snapshot" && call.Args[1] == "create" && call.WorkingDir != dataPath {
			t.Fatalf("snapshot create working directory mismatch: got=%q want=%q", call.WorkingDir, dataPath)
		}
		if !(len(call.Args) >= 2 && call.Args[0] == "snapshot" && call.Args[1] == "create") && call.WorkingDir != repoPath {
			t.Fatalf("ordinary command working directory mismatch: args=%v got=%q want=%q", call.Args, call.WorkingDir, repoPath)
		}
	}
	if !containsCommand(joined, "gc") {
		t.Fatalf("expected gc command, got=%v", joined)
	}
}

func TestScenarioSnapshotCreationUsesDataSelectionBase(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:                runner.run,
		ColdkeepExecutable:    "coldkeep",
		MixedFileCount:        2,
		MixedMinFileSizeBytes: 64,
		MixedMaxFileSizeBytes: 128,
	}
	repoPath := t.TempDir()
	dataPath := t.TempDir()
	scenario := scenarioByName(t, CoreScenarios(cfg), "snapshot-creation")

	if err := scenario.Run(BenchmarkContext{RepoPath: repoPath, DataPath: dataPath}); err != nil {
		t.Fatalf("scenario run failed: %v", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("expected store and snapshot calls, got %d", len(runner.calls))
	}
	if got := strings.Join(runner.calls[1].Args, " "); got != "snapshot create snapshot/ --id bench-snapshot-core" {
		t.Fatalf("snapshot command mismatch: got=%q", got)
	}
	if runner.calls[1].WorkingDir != dataPath {
		t.Fatalf("snapshot create working directory mismatch: got=%q want=%q", runner.calls[1].WorkingDir, dataPath)
	}
	if runner.calls[0].WorkingDir != repoPath {
		t.Fatalf("store command working directory mismatch: got=%q want=%q", runner.calls[0].WorkingDir, repoPath)
	}
}

func TestScenarioStatsInspectRunsBothCommands(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:                runner.run,
		ColdkeepExecutable:    "coldkeep",
		MixedFileCount:        4,
		MixedMinFileSizeBytes: 64,
		MixedMaxFileSizeBytes: 128,
	}
	scenario := scenarioByName(t, CoreScenarios(cfg), "stats-inspect")

	if err := scenario.Run(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}); err != nil {
		t.Fatalf("scenario run failed: %v", err)
	}

	joined := runner.joinedCommands()
	if !containsCommand(joined, "stats") {
		t.Fatalf("expected stats command, got=%v", joined)
	}
	if !containsCommand(joined, "inspect repository") {
		t.Fatalf("expected inspect repository command, got=%v", joined)
	}

	for _, call := range runner.calls {
		if !strings.HasPrefix(call.WorkingDir, filepath.Clean(call.WorkingDir)) {
			t.Fatalf("unexpected working dir %q", call.WorkingDir)
		}
	}
}

func TestScenarioVerifySystemDeepRunsVerifyCommand(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:                runner.run,
		ColdkeepExecutable:    "coldkeep",
		MixedFileCount:        4,
		MixedMinFileSizeBytes: 64,
		MixedMaxFileSizeBytes: 128,
	}
	scenario := scenarioByName(t, CoreScenarios(cfg), "verify-system-deep")

	if err := scenario.Run(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}); err != nil {
		t.Fatalf("scenario run failed: %v", err)
	}

	joined := runner.joinedCommands()
	if !containsCommand(joined, "store-folder") {
		t.Fatalf("expected store-folder command, got=%v", joined)
	}
	if !containsCommand(joined, "verify system --deep") {
		t.Fatalf("expected verify system --deep command, got=%v", joined)
	}
}

func TestRunColdkeepSetsCompressionNoneByDefaultStep37(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:             runner.run,
		ColdkeepExecutable: "coldkeep",
	}

	if err := runColdkeep(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}, cfg, "stats"); err != nil {
		t.Fatalf("runColdkeep: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected one command call, got %d", len(runner.calls))
	}
	if got := envValue(runner.calls[0].Env, "COLDKEEP_COMPRESSION"); got != "none" {
		t.Fatalf("expected COLDKEEP_COMPRESSION=none, got %q", got)
	}
}

func TestRunColdkeepAllowsCompressionOverrideStep37(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:             runner.run,
		ColdkeepExecutable: "coldkeep",
		ExtraEnv: map[string]string{
			"COLDKEEP_COMPRESSION": "zstd",
		},
	}

	if err := runColdkeep(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}, cfg, "stats"); err != nil {
		t.Fatalf("runColdkeep: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected one command call, got %d", len(runner.calls))
	}
	if got := envValue(runner.calls[0].Env, "COLDKEEP_COMPRESSION"); got != "zstd" {
		t.Fatalf("expected COLDKEEP_COMPRESSION override=zstd, got %q", got)
	}
}

func TestRunColdkeepUsesConfiguredCompressionStep37(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:             runner.run,
		ColdkeepExecutable: "coldkeep",
		Compression:        "zstd",
	}

	if err := runColdkeep(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}, cfg, "stats"); err != nil {
		t.Fatalf("runColdkeep: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected one command call, got %d", len(runner.calls))
	}
	if got := envValue(runner.calls[0].Env, "COLDKEEP_COMPRESSION"); got != "zstd" {
		t.Fatalf("expected configured COLDKEEP_COMPRESSION=zstd, got %q", got)
	}
}

func TestRunColdkeepUsesConfiguredCodecStep37(t *testing.T) {
	runner := &captureRunner{}
	cfg := ScenarioConfig{
		Runner:             runner.run,
		ColdkeepExecutable: "coldkeep",
		Codec:              "aes-gcm",
	}

	if err := runColdkeep(BenchmarkContext{RepoPath: t.TempDir(), DataPath: t.TempDir()}, cfg, "stats"); err != nil {
		t.Fatalf("runColdkeep: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("expected one command call, got %d", len(runner.calls))
	}
	if got := envValue(runner.calls[0].Env, "COLDKEEP_CODEC"); got != "aes-gcm" {
		t.Fatalf("expected configured COLDKEEP_CODEC=aes-gcm, got %q", got)
	}
}

type captureRunner struct {
	calls []CommandSpec
}

func (c *captureRunner) run(spec CommandSpec) error {
	clone := CommandSpec{
		Executable: spec.Executable,
		Args:       append([]string(nil), spec.Args...),
		WorkingDir: spec.WorkingDir,
		Env:        append([]string(nil), spec.Env...),
	}
	c.calls = append(c.calls, clone)
	return nil
}

func (c *captureRunner) joinedCommands() []string {
	out := make([]string, 0, len(c.calls))
	for _, call := range c.calls {
		out = append(out, strings.TrimSpace(strings.Join(call.Args, " ")))
	}
	return out
}

func containsCommand(commands []string, want string) bool {
	for _, command := range commands {
		if strings.Contains(command, want) {
			return true
		}
	}
	return false
}

func scenarioByName(t *testing.T, scenarios []BenchmarkCase, name string) BenchmarkCase {
	t.Helper()
	for _, scenario := range scenarios {
		if scenario.Name == name {
			return scenario
		}
	}
	t.Fatalf("scenario %q not found", name)
	return BenchmarkCase{}
}
