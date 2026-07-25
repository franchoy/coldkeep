package benchmark

import "testing"

func TestParseDatasetPreset(t *testing.T) {
	preset, err := ParseDatasetPreset("")
	if err != nil {
		t.Fatalf("ParseDatasetPreset empty: %v", err)
	}
	if preset != DatasetPresetSmall {
		t.Fatalf("expected default small, got %q", preset)
	}

	preset, err = ParseDatasetPreset("MeDiUm")
	if err != nil {
		t.Fatalf("ParseDatasetPreset medium: %v", err)
	}
	if preset != DatasetPresetMedium {
		t.Fatalf("expected medium, got %q", preset)
	}

	preset, err = ParseDatasetPreset("CI-STABLE-V1")
	if err != nil {
		t.Fatalf("ParseDatasetPreset ci-stable-v1: %v", err)
	}
	if preset != DatasetPresetCIStableV1 {
		t.Fatalf("expected ci-stable-v1, got %q", preset)
	}

	if _, err := ParseDatasetPreset("xlarge"); err == nil {
		t.Fatal("expected invalid preset error")
	}
}

func TestRunPresetValidatesRepeat(t *testing.T) {
	_, err := RunPreset(DatasetPresetSmall, 0, ScenarioConfig{})
	if err == nil {
		t.Fatal("expected repeat validation error")
	}
}

func TestRunPresetCIStableV1RequiresCaseEnvironmentFactory(t *testing.T) {
	_, err := RunPreset(DatasetPresetCIStableV1, 1, ScenarioConfig{})
	if err == nil || err.Error() != `preset "ci-stable-v1" requires a per-case environment factory` {
		t.Fatalf("expected case isolation requirement, got: %v", err)
	}
}

func TestRunPresetWithStubRunner(t *testing.T) {
	calls := 0
	report, err := RunPreset(DatasetPresetSmall, 2, ScenarioConfig{
		ColdkeepExecutable: "coldkeep",
		Runner: func(spec CommandSpec) error {
			calls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RunPreset returned error: %v", err)
	}
	if report.Dataset != DatasetPresetSmall || report.Repeat != 2 {
		t.Fatalf("unexpected report header: %+v", report)
	}
	if report.Fixture.ID != string(DatasetPresetSmall) {
		t.Fatalf("unexpected fixture descriptor: %+v", report.Fixture)
	}
	if len(report.Iterations) != 2 {
		t.Fatalf("expected 2 iterations, got %d", len(report.Iterations))
	}
	if calls == 0 {
		t.Fatal("expected scenario command calls")
	}
}
