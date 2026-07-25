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
