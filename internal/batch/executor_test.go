package batch

import "testing"

func TestExecutePreparedPreservesInputOrderAndFailFast(t *testing.T) {
	targets := []PreparedTarget{
		{ID: 12, Executable: true},
		{Executable: false, Result: ItemResult{RawValue: "abc", Status: ResultFailed, Message: "invalid file ID \"abc\""}},
		{ID: 18, Executable: true},
		{ID: 12, Executable: false, Result: ItemResult{ID: 12, Status: ResultSkipped, Message: "duplicate target"}},
	}

	report := ExecutePrepared(OperationRemove, false, true, targets, func(id int64) ItemResult {
		if id == 18 {
			return ItemResult{ID: id, Status: ResultFailed, Message: "forced failure"}
		}
		return ItemResult{ID: id, Status: ResultSuccess, Message: "ok"}
	})
	if report.ExecutionMode != ExecutionModeFailFast {
		t.Fatalf("expected execution_mode=%s, got %s", ExecutionModeFailFast, report.ExecutionMode)
	}

	if len(report.Results) != 3 {
		t.Fatalf("expected fail-fast to stop after third result, got %d", len(report.Results))
	}

	if report.Results[0].ID != 12 || report.Results[0].Status != ResultSuccess {
		t.Fatalf("unexpected first result: %+v", report.Results[0])
	}
	if report.Results[1].Status != ResultFailed || report.Results[1].RawValue != "abc" {
		t.Fatalf("unexpected second result: %+v", report.Results[1])
	}
	if report.Results[2].ID != 18 || report.Results[2].Status != ResultFailed {
		t.Fatalf("unexpected third result: %+v", report.Results[2])
	}
}

func TestExecutePreparedFailFastStopsOnlyOnExecutionFailure(t *testing.T) {
	targets := []PreparedTarget{
		{Executable: false, Result: ItemResult{RawValue: "abc", Status: ResultFailed, Message: "invalid file ID \"abc\""}},
		{ID: 12, Executable: true},
		{ID: 18, Executable: true},
	}

	report := ExecutePrepared(OperationRemove, false, true, targets, func(id int64) ItemResult {
		if id == 18 {
			return ItemResult{ID: id, Status: ResultFailed, Message: "forced execution failure"}
		}
		return ItemResult{ID: id, Status: ResultSuccess, Message: "ok"}
	})
	if report.ExecutionMode != ExecutionModeFailFast {
		t.Fatalf("expected execution_mode=%s, got %s", ExecutionModeFailFast, report.ExecutionMode)
	}

	if len(report.Results) != 3 {
		t.Fatalf("expected all three ordered results, got %d", len(report.Results))
	}
	if report.Results[0].Status != ResultFailed || report.Results[0].RawValue != "abc" {
		t.Fatalf("expected first pre-execution parse failure result, got %+v", report.Results[0])
	}
	if report.Results[1].Status != ResultSuccess || report.Results[1].ID != 12 {
		t.Fatalf("expected second result to execute successfully, got %+v", report.Results[1])
	}
	if report.Results[2].Status != ResultFailed || report.Results[2].ID != 18 {
		t.Fatalf("expected third result to be execution failure, got %+v", report.Results[2])
	}
}
