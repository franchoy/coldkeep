package benchmark

const (
	// CIStableV1FixtureID identifies the fixed Phase 11 calibration candidate.
	// Changing any value in this profile requires a new fixture identifier.
	CIStableV1FixtureID = "ci-stable-v1"
	FixtureSeed         = int64(1701)
)

// FixtureCase identifies one ordered benchmark case and its deterministic seed.
type FixtureCase struct {
	Name string `json:"name"`
	Seed int64  `json:"seed"`
}

// FixtureDescriptor records the inputs that define a benchmark fixture.
type FixtureDescriptor struct {
	ID                     string        `json:"id"`
	Seed                   int64         `json:"seed"`
	LargeFileSizeBytes     int64         `json:"large_file_size_bytes"`
	ManySmallFileCount     int           `json:"many_small_file_count"`
	ManySmallFileSizeBytes int           `json:"many_small_file_size_bytes"`
	MixedFileCount         int           `json:"mixed_file_count"`
	MixedMinFileSizeBytes  int           `json:"mixed_min_file_size_bytes"`
	MixedMaxFileSizeBytes  int           `json:"mixed_max_file_size_bytes"`
	RemoveEvery            int           `json:"remove_every"`
	CaseDatabaseIsolation  bool          `json:"case_database_isolation"`
	OrderedCases           []FixtureCase `json:"ordered_cases"`
}

// CIStableV1ScenarioConfig is the fixed release-gate calibration candidate.
func CIStableV1ScenarioConfig() ScenarioConfig {
	return ScenarioConfig{
		Seed:                   FixtureSeed,
		LargeFileSizeBytes:     96 * 1024 * 1024,
		ManySmallFileCount:     600,
		ManySmallFileSizeBytes: 1024,
		MixedFileCount:         400,
		MixedMinFileSizeBytes:  1024,
		MixedMaxFileSizeBytes:  256 * 1024,
		RemoveEvery:            4,
		CaseDatabaseIsolation:  true,
	}
}

// FixtureDescriptorFor returns the deterministic descriptor for a preset.
func FixtureDescriptorFor(preset DatasetPreset, cfg ScenarioConfig) FixtureDescriptor {
	cfg = cfg.withDefaults()
	id := string(preset)
	if preset == DatasetPresetCIStableV1 {
		id = CIStableV1FixtureID
	}
	return FixtureDescriptor{
		ID:                     id,
		Seed:                   cfg.Seed,
		LargeFileSizeBytes:     cfg.LargeFileSizeBytes,
		ManySmallFileCount:     cfg.ManySmallFileCount,
		ManySmallFileSizeBytes: cfg.ManySmallFileSizeBytes,
		MixedFileCount:         cfg.MixedFileCount,
		MixedMinFileSizeBytes:  cfg.MixedMinFileSizeBytes,
		MixedMaxFileSizeBytes:  cfg.MixedMaxFileSizeBytes,
		RemoveEvery:            cfg.RemoveEvery,
		CaseDatabaseIsolation:  cfg.CaseDatabaseIsolation,
		OrderedCases: []FixtureCase{
			{Name: "store-large-file", Seed: cfg.Seed + 11},
			{Name: "store-many-small-files", Seed: cfg.Seed + 21},
			{Name: "store-mixed-dataset", Seed: cfg.Seed + 31},
			{Name: "restore-large-file", Seed: cfg.Seed + 41},
			{Name: "restore-many-files", Seed: cfg.Seed + 51},
			{Name: "snapshot-creation", Seed: cfg.Seed + 61},
			{Name: "gc-after-churn", Seed: cfg.Seed + 71},
			{Name: "stats-inspect", Seed: cfg.Seed + 81},
			{Name: "verify-system-deep", Seed: cfg.Seed + 91},
		},
	}
}
