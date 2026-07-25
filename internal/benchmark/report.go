package benchmark

import (
	"fmt"
	"strings"
	"time"
)

type DatasetPreset string

const (
	DatasetPresetSmall      DatasetPreset = "small"
	DatasetPresetMedium     DatasetPreset = "medium"
	DatasetPresetLarge      DatasetPreset = "large"
	DatasetPresetCIStableV1 DatasetPreset = CIStableV1FixtureID
)

type IterationReport struct {
	Iteration int      `json:"iteration"`
	Results   []Result `json:"results"`
}

type RunReport struct {
	GeneratedAtUTC string            `json:"generated_at_utc"`
	Dataset        DatasetPreset     `json:"dataset"`
	Repeat         int               `json:"repeat"`
	Fixture        FixtureDescriptor `json:"fixture"`
	Iterations     []IterationReport `json:"iterations"`
}

func ParseDatasetPreset(raw string) (DatasetPreset, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return DatasetPresetSmall, nil
	}

	switch DatasetPreset(normalized) {
	case DatasetPresetSmall, DatasetPresetMedium, DatasetPresetLarge, DatasetPresetCIStableV1:
		return DatasetPreset(normalized), nil
	default:
		return "", fmt.Errorf("invalid dataset preset %q (allowed: small, medium, large, ci-stable-v1)", raw)
	}
}

func PresetScenarioConfig(preset DatasetPreset) (ScenarioConfig, error) {
	switch preset {
	case DatasetPresetSmall:
		return ScenarioConfig{
			LargeFileSizeBytes:     16 * 1024 * 1024,
			ManySmallFileCount:     100,
			ManySmallFileSizeBytes: 1024,
			MixedFileCount:         20,
			MixedMinFileSizeBytes:  1024,
			MixedMaxFileSizeBytes:  256 * 1024,
			RemoveEvery:            4,
		}, nil
	case DatasetPresetMedium:
		return ScenarioConfig{
			LargeFileSizeBytes:     256 * 1024 * 1024,
			ManySmallFileCount:     1000,
			ManySmallFileSizeBytes: 4 * 1024,
			MixedFileCount:         100,
			MixedMinFileSizeBytes:  1024,
			MixedMaxFileSizeBytes:  10 * 1024 * 1024,
			RemoveEvery:            3,
		}, nil
	case DatasetPresetLarge:
		return ScenarioConfig{
			LargeFileSizeBytes:     defaultLargeFileSizeBytes,
			ManySmallFileCount:     defaultManySmallFileCount,
			ManySmallFileSizeBytes: defaultManySmallFileSizeBytes,
			MixedFileCount:         defaultMixedFileCount,
			MixedMinFileSizeBytes:  defaultMixedMinFileSizeBytes,
			MixedMaxFileSizeBytes:  defaultMixedMaxFileSizeBytes,
			RemoveEvery:            defaultRemoveEvery,
		}, nil
	case DatasetPresetCIStableV1:
		return CIStableV1ScenarioConfig(), nil
	default:
		return ScenarioConfig{}, fmt.Errorf("unsupported dataset preset %q", preset)
	}
}

func RunPreset(preset DatasetPreset, repeat int, base ScenarioConfig) (RunReport, error) {
	if repeat <= 0 {
		return RunReport{}, fmt.Errorf("repeat must be > 0")
	}

	presetCfg, err := PresetScenarioConfig(preset)
	if err != nil {
		return RunReport{}, err
	}

	cfg := base
	cfg.LargeFileSizeBytes = presetCfg.LargeFileSizeBytes
	cfg.ManySmallFileCount = presetCfg.ManySmallFileCount
	cfg.ManySmallFileSizeBytes = presetCfg.ManySmallFileSizeBytes
	cfg.MixedFileCount = presetCfg.MixedFileCount
	cfg.MixedMinFileSizeBytes = presetCfg.MixedMinFileSizeBytes
	cfg.MixedMaxFileSizeBytes = presetCfg.MixedMaxFileSizeBytes
	cfg.RemoveEvery = presetCfg.RemoveEvery
	cfg.CaseDatabaseIsolation = presetCfg.CaseDatabaseIsolation
	if cfg.CaseDatabaseIsolation && cfg.CaseEnvironmentFactory == nil {
		return RunReport{}, fmt.Errorf("preset %q requires a per-case environment factory", preset)
	}

	report := RunReport{
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Dataset:        preset,
		Repeat:         repeat,
		Fixture:        FixtureDescriptorFor(preset, cfg),
		Iterations:     make([]IterationReport, 0, repeat),
	}

	for i := 1; i <= repeat; i++ {
		iterCfg := cfg
		iterCfg.RunTag = fmt.Sprintf("iter-%02d", i)
		var results []Result
		var runErr error
		if iterCfg.CaseEnvironmentFactory != nil {
			results, runErr = RunBenchmarkWithEnvironmentFactory(CoreScenarios(iterCfg), iterCfg.CaseEnvironmentFactory)
		} else {
			results, runErr = RunBenchmark(CoreScenarios(iterCfg))
		}
		report.Iterations = append(report.Iterations, IterationReport{
			Iteration: i,
			Results:   results,
		})
		if runErr != nil {
			return report, runErr
		}
	}

	return report, nil
}
