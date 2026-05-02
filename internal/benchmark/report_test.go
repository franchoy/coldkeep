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
	if len(report.Iterations) != 2 {
		t.Fatalf("expected 2 iterations, got %d", len(report.Iterations))
	}
	if calls == 0 {
		t.Fatal("expected scenario command calls")
	}
}
