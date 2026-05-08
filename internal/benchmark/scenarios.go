package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/franchoy/coldkeep/internal/execution"
)

const (
	defaultLargeFileSizeBytes     int64 = 1 * 1024 * 1024 * 1024
	defaultManySmallFileCount           = 1000
	defaultManySmallFileSizeBytes       = 4 * 1024
	defaultMixedFileCount               = 100
	defaultMixedMinFileSizeBytes        = 1 * 1024
	defaultMixedMaxFileSizeBytes        = 10 * 1024 * 1024
	defaultRemoveEvery                  = 3
	defaultSeed                   int64 = 1701
)

// CommandSpec describes a coldkeep command invocation used by benchmark scenarios.
type CommandSpec struct {
	Executable string
	Args       []string
	WorkingDir string
	Env        []string
}

// CommandRunner executes one benchmark command.
type CommandRunner func(spec CommandSpec) error

// ScenarioConfig controls Step 4 core benchmark scenarios.
type ScenarioConfig struct {
	ColdkeepExecutable     string
	Codec                  string
	Execution              execution.Options
	Seed                   int64
	LargeFileSizeBytes     int64
	ManySmallFileCount     int
	ManySmallFileSizeBytes int
	MixedFileCount         int
	MixedMinFileSizeBytes  int
	MixedMaxFileSizeBytes  int
	RemoveEvery            int
	RunTag                 string
	ExtraEnv               map[string]string
	Runner                 CommandRunner
}

// CoreScenarios returns the v1.7 Step 4 real-world benchmark cases.
func CoreScenarios(cfg ScenarioConfig) []BenchmarkCase {
	cfg = cfg.withDefaults()

	return []BenchmarkCase{
		{Name: "store-large-file", Run: scenarioStoreLargeFile(cfg), Execution: cfg.Execution},
		{Name: "store-many-small-files", Run: scenarioStoreManySmallFiles(cfg), Execution: cfg.Execution},
		{Name: "store-mixed-dataset", Run: scenarioStoreMixedDataset(cfg), Execution: cfg.Execution},
		{Name: "restore-large-file", Run: scenarioRestoreLargeFile(cfg), Execution: cfg.Execution},
		{Name: "restore-many-files", Run: scenarioRestoreManyFiles(cfg), Execution: cfg.Execution},
		{Name: "snapshot-creation", Run: scenarioSnapshotCreation(cfg), Execution: cfg.Execution},
		{Name: "gc-after-churn", Run: scenarioGCAfterChurn(cfg), Execution: cfg.Execution},
		{Name: "stats-inspect", Run: scenarioStatsInspect(cfg), Execution: cfg.Execution},
		{Name: "verify-system-deep", Run: scenarioVerifySystemDeep(cfg), Execution: cfg.Execution},
	}
}

func (c ScenarioConfig) withDefaults() ScenarioConfig {
	if strings.TrimSpace(c.ColdkeepExecutable) == "" {
		c.ColdkeepExecutable = "coldkeep"
	}
	if strings.TrimSpace(c.Codec) == "" {
		c.Codec = "plain"
	}
	if c.Seed == 0 {
		c.Seed = defaultSeed
	}
	if c.LargeFileSizeBytes <= 0 {
		c.LargeFileSizeBytes = defaultLargeFileSizeBytes
	}
	if c.ManySmallFileCount <= 0 {
		c.ManySmallFileCount = defaultManySmallFileCount
	}
	if c.ManySmallFileSizeBytes <= 0 {
		c.ManySmallFileSizeBytes = defaultManySmallFileSizeBytes
	}
	if c.MixedFileCount <= 0 {
		c.MixedFileCount = defaultMixedFileCount
	}
	if c.MixedMinFileSizeBytes <= 0 {
		c.MixedMinFileSizeBytes = defaultMixedMinFileSizeBytes
	}
	if c.MixedMaxFileSizeBytes <= 0 {
		c.MixedMaxFileSizeBytes = defaultMixedMaxFileSizeBytes
	}
	if c.MixedMaxFileSizeBytes < c.MixedMinFileSizeBytes {
		c.MixedMaxFileSizeBytes = c.MixedMinFileSizeBytes
	}
	if c.RemoveEvery <= 0 {
		c.RemoveEvery = defaultRemoveEvery
	}
	if c.Runner == nil {
		c.Runner = defaultCommandRunner
	}
	return c
}

func scenarioStoreLargeFile(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		largePath := filepath.Join(ctx.DataPath, "large", "file.bin")
		if err := writeDeterministicFile(largePath, cfg.LargeFileSizeBytes, cfg.Seed+11); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store", "--codec", cfg.Codec, largePath); err != nil {
			return err
		}
		RecordProcessed(1, cfg.LargeFileSizeBytes)
		return nil
	}
}

func scenarioStoreManySmallFiles(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "many-small")
		bytesTotal, err := createUniformDataset(datasetDir, cfg.ManySmallFileCount, cfg.ManySmallFileSizeBytes, cfg.Seed+21, "mixed")
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		RecordProcessed(cfg.ManySmallFileCount, bytesTotal)
		return nil
	}
}

func scenarioStoreMixedDataset(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "mixed")
		paths, bytesTotal, err := createMixedSizeDataset(datasetDir, cfg.MixedFileCount, cfg.MixedMinFileSizeBytes, cfg.MixedMaxFileSizeBytes, cfg.Seed+31)
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func scenarioRestoreLargeFile(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		largePath := filepath.Join(ctx.DataPath, "restore-large", "file.bin")
		if err := writeDeterministicFile(largePath, cfg.LargeFileSizeBytes, cfg.Seed+41); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store", "--codec", cfg.Codec, largePath); err != nil {
			return err
		}

		restoreDest := filepath.Join(ctx.RepoPath, "restore-output", "large", "file.bin")
		if err := os.MkdirAll(filepath.Dir(restoreDest), 0o755); err != nil {
			return fmt.Errorf("create restore destination dir: %w", err)
		}
		if err := runColdkeep(ctx, cfg,
			"restore",
			"--stored-path", largePath,
			"--mode", "override",
			"--destination", restoreDest,
			"--overwrite",
		); err != nil {
			return err
		}

		RecordProcessed(1, cfg.LargeFileSizeBytes)
		return nil
	}
}

func scenarioRestoreManyFiles(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "restore-many")
		paths, bytesTotal, err := createUniformDatasetPaths(datasetDir, cfg.ManySmallFileCount, cfg.ManySmallFileSizeBytes, cfg.Seed+51, "mixed")
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}

		restoreRoot := filepath.Join(ctx.RepoPath, "restore-output", "many")
		if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
			return fmt.Errorf("create restore root: %w", err)
		}

		for _, src := range paths {
			dst := filepath.Join(restoreRoot, filepath.Base(src))
			if err := runColdkeep(ctx, cfg,
				"restore",
				"--stored-path", src,
				"--mode", "override",
				"--destination", dst,
				"--overwrite",
			); err != nil {
				return err
			}
		}

		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func scenarioSnapshotCreation(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "snapshot")
		paths, bytesTotal, err := createMixedSizeDataset(datasetDir, cfg.MixedFileCount, cfg.MixedMinFileSizeBytes, cfg.MixedMaxFileSizeBytes, cfg.Seed+61)
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "snapshot", "create", "--id", snapshotID("bench-snapshot-core", cfg.RunTag)); err != nil {
			return err
		}

		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func scenarioGCAfterChurn(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "churn")
		paths, bytesTotal, err := createUniformDatasetPaths(datasetDir, cfg.ManySmallFileCount, cfg.ManySmallFileSizeBytes, cfg.Seed+71, "mixed")
		if err != nil {
			return err
		}

		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		for i, path := range paths {
			if (i+1)%cfg.RemoveEvery != 0 {
				continue
			}
			if err := runColdkeep(ctx, cfg, "remove", "--stored-path", path); err != nil {
				return err
			}
		}

		if err := runColdkeep(ctx, cfg, "snapshot", "create", "--id", snapshotID("bench-snapshot-gc", cfg.RunTag)); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "gc"); err != nil {
			return err
		}

		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func scenarioStatsInspect(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "stats")
		paths, bytesTotal, err := createMixedSizeDataset(datasetDir, cfg.MixedFileCount, cfg.MixedMinFileSizeBytes, cfg.MixedMaxFileSizeBytes, cfg.Seed+81)
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "stats"); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "inspect", "repository"); err != nil {
			return err
		}

		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func scenarioVerifySystemDeep(cfg ScenarioConfig) func(ctx BenchmarkContext) error {
	return func(ctx BenchmarkContext) error {
		datasetDir := filepath.Join(ctx.DataPath, "verify")
		paths, bytesTotal, err := createMixedSizeDataset(datasetDir, cfg.MixedFileCount, cfg.MixedMinFileSizeBytes, cfg.MixedMaxFileSizeBytes, cfg.Seed+91)
		if err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "store-folder", "--codec", cfg.Codec, datasetDir); err != nil {
			return err
		}
		if err := runColdkeep(ctx, cfg, "verify", "system", "--deep"); err != nil {
			return err
		}

		RecordProcessed(len(paths), bytesTotal)
		return nil
	}
}

func createUniformDataset(dir string, fileCount int, fileSize int, seed int64, pattern string) (int64, error) {
	if err := GenerateDataset(dir, DatasetConfig{
		NumFiles:      fileCount,
		FileSizeBytes: fileSize,
		Pattern:       pattern,
		Seed:          seed,
	}); err != nil {
		return 0, err
	}
	return int64(fileCount) * int64(fileSize), nil
}

func createUniformDatasetPaths(dir string, fileCount int, fileSize int, seed int64, pattern string) ([]string, int64, error) {
	if _, err := createUniformDataset(dir, fileCount, fileSize, seed, pattern); err != nil {
		return nil, 0, err
	}

	paths := make([]string, 0, fileCount)
	for i := 1; i <= fileCount; i++ {
		paths = append(paths, filepath.Join(dir, fmt.Sprintf("file_%04d.bin", i)))
	}
	return paths, int64(fileCount) * int64(fileSize), nil
}

func createMixedSizeDataset(dir string, fileCount int, minSize int, maxSize int, seed int64) ([]string, int64, error) {
	if fileCount <= 0 {
		return nil, 0, fmt.Errorf("mixed dataset file count must be > 0")
	}
	if minSize <= 0 || maxSize <= 0 {
		return nil, 0, fmt.Errorf("mixed dataset sizes must be > 0")
	}
	if maxSize < minSize {
		return nil, 0, fmt.Errorf("mixed dataset max size must be >= min size")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, fmt.Errorf("create mixed dataset dir %q: %w", dir, err)
	}

	rng := rand.New(rand.NewSource(seed))
	paths := make([]string, 0, fileCount)
	var totalBytes int64
	for i := 1; i <= fileCount; i++ {
		size := minSize
		if maxSize > minSize {
			size += rng.Intn(maxSize - minSize + 1)
		}
		path := filepath.Join(dir, fmt.Sprintf("mixed_%04d.bin", i))
		if err := writeDeterministicFile(path, int64(size), seed+int64(i)*17); err != nil {
			return nil, 0, err
		}
		paths = append(paths, path)
		totalBytes += int64(size)
	}

	return paths, totalBytes, nil
}

// WriteDeterministicFile writes size bytes of deterministic pseudo-random content
// derived from seed to path. It is exported so that external validation helpers
// (e.g. restore-hash determinism checks) can produce the same byte sequence.
func WriteDeterministicFile(path string, size int64, seed int64) error {
	return writeDeterministicFile(path, size, seed)
}

func writeDeterministicFile(path string, size int64, seed int64) error {
	if size <= 0 {
		return fmt.Errorf("file size must be > 0")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create file parent dir for %q: %w", path, err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, 64*1024)
	var written int64
	for written < size {
		for i := range buf {
			buf[i] = byte(rng.Intn(256))
		}
		toWrite := int64(len(buf))
		remaining := size - written
		if remaining < toWrite {
			toWrite = remaining
		}
		n, err := file.Write(buf[:toWrite])
		if err != nil {
			return fmt.Errorf("write file %q: %w", path, err)
		}
		written += int64(n)
	}

	return nil
}

func runColdkeep(ctx BenchmarkContext, cfg ScenarioConfig, args ...string) error {
	if strings.TrimSpace(cfg.ColdkeepExecutable) == "" {
		return fmt.Errorf("coldkeep executable cannot be empty")
	}

	env := withScenarioEnv(os.Environ(), map[string]string{
		"COLDKEEP_STORAGE_DIR": filepath.Join(ctx.RepoPath, "storage", "containers"),
		"COLDKEEP_CODEC":       cfg.Codec,
		"COLDKEEP_COMPRESSION": "none",
	}, cfg.ExtraEnv)

	spec := CommandSpec{
		Executable: cfg.ColdkeepExecutable,
		Args:       append([]string(nil), args...),
		WorkingDir: ctx.RepoPath,
		Env:        env,
	}

	if err := cfg.Runner(spec); err != nil {
		return fmt.Errorf("run coldkeep %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func snapshotID(base string, runTag string) string {
	tag := strings.TrimSpace(runTag)
	if tag == "" {
		return base
	}
	return base + "-" + tag
}

func defaultCommandRunner(spec CommandSpec) error {
	cmd := exec.Command(spec.Executable, spec.Args...)
	cmd.Dir = spec.WorkingDir
	cmd.Env = spec.Env
	output, err := cmd.CombinedOutput()
	if err != nil {
		if len(output) == 0 {
			return err
		}
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func withScenarioEnv(base []string, baseOverrides map[string]string, extra map[string]string) []string {
	merged := make(map[string]string, len(base)+len(baseOverrides)+len(extra))
	for _, item := range base {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			continue
		}
		merged[parts[0]] = parts[1]
	}
	for k, v := range baseOverrides {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}

	out := make([]string, 0, len(merged))
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, k+"="+merged[k])
	}
	return out
}
