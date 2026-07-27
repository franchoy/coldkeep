package benchmark

import "testing"

func TestCIStableV1FixtureContract(t *testing.T) {
	cfg := CIStableV1ScenarioConfig()
	if cfg.Seed != 1701 ||
		cfg.LargeFileSizeBytes != 96*1024*1024 ||
		cfg.ManySmallFileCount != 600 ||
		cfg.ManySmallFileSizeBytes != 1024 ||
		cfg.MixedFileCount != 400 ||
		cfg.MixedMinFileSizeBytes != 1024 ||
		cfg.MixedMaxFileSizeBytes != 256*1024 ||
		cfg.RemoveEvery != 4 ||
		!cfg.CaseDatabaseIsolation {
		t.Fatalf("unexpected ci-stable-v1 config: %+v", cfg)
	}

	descriptor := FixtureDescriptorFor(DatasetPresetCIStableV1, cfg)
	if descriptor.ID != CIStableV1FixtureID || !descriptor.CaseDatabaseIsolation {
		t.Fatalf("unexpected descriptor: %+v", descriptor)
	}
	wantCases := []string{
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
	if len(descriptor.OrderedCases) != len(wantCases) {
		t.Fatalf("case count: got=%d want=%d", len(descriptor.OrderedCases), len(wantCases))
	}
	for index, want := range wantCases {
		got := descriptor.OrderedCases[index]
		if got.Name != want {
			t.Fatalf("case %d: got=%q want=%q", index, got.Name, want)
		}
		wantSeed := FixtureSeed + int64((index+1)*10+1)
		if got.Seed != wantSeed {
			t.Fatalf("seed for %s: got=%d want=%d", got.Name, got.Seed, wantSeed)
		}
	}
}

func TestCIPairedFixtureContracts(t *testing.T) {
	tests := []struct {
		name       string
		preset     DatasetPreset
		fixtureID  string
		config     ScenarioConfig
		largeBytes int64
		manyCount  int
		mixedCount int
	}{
		{
			name:       "workers-1",
			preset:     DatasetPresetCIPairedW1V1,
			fixtureID:  CIPairedW1V1FixtureID,
			config:     CIPairedW1V1ScenarioConfig(),
			largeBytes: 96 * 1024 * 1024,
			manyCount:  600,
			mixedCount: 400,
		},
		{
			name:       "workers-4",
			preset:     DatasetPresetCIPairedW4V1,
			fixtureID:  CIPairedW4V1FixtureID,
			config:     CIPairedW4V1ScenarioConfig(),
			largeBytes: 128 * 1024 * 1024,
			manyCount:  1200,
			mixedCount: 800,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := test.config
			if cfg.Seed != FixtureSeed ||
				cfg.LargeFileSizeBytes != test.largeBytes ||
				cfg.ManySmallFileCount != test.manyCount ||
				cfg.ManySmallFileSizeBytes != 1024 ||
				cfg.MixedFileCount != test.mixedCount ||
				cfg.MixedMinFileSizeBytes != 1024 ||
				cfg.MixedMaxFileSizeBytes != 256*1024 ||
				cfg.RemoveEvery != 4 ||
				!cfg.CaseDatabaseIsolation {
				t.Fatalf("unexpected paired config: %+v", cfg)
			}
			descriptor := FixtureDescriptorFor(test.preset, cfg)
			if descriptor.ID != test.fixtureID {
				t.Fatalf("unexpected descriptor: %+v", descriptor)
			}
			wantCases := []string{
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
			if len(descriptor.OrderedCases) != len(wantCases) {
				t.Fatalf("case count: got=%d want=%d", len(descriptor.OrderedCases), len(wantCases))
			}
			for index, want := range wantCases {
				got := descriptor.OrderedCases[index]
				if got.Name != want {
					t.Fatalf("case %d: got=%q want=%q", index, got.Name, want)
				}
				wantSeed := FixtureSeed + int64((index+1)*10+1)
				if got.Seed != wantSeed {
					t.Fatalf("seed for %s: got=%d want=%d", got.Name, got.Seed, wantSeed)
				}
			}
			if !RequiresCaseDatabaseIsolation(test.preset) {
				t.Fatalf("paired preset must require isolation: %q", test.preset)
			}
		})
	}
}
