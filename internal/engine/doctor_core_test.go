package engine

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDoctorOwnsOrderedCompositeAndNeutralResult(t *testing.T) {
	eng := newDoctorHookEngine(t)
	var calls []string
	eng.doctorRecover = func(context.Context) (RecoverResult, error) {
		calls = append(calls, "recovery")
		return RecoverResult{AbortedLogicalFiles: 2}, nil
	}
	eng.doctorSchema = func(*sql.DB) (int64, error) {
		calls = append(calls, "schema")
		return 8, nil
	}
	eng.doctorVerify = func(_ context.Context, level string) error {
		calls = append(calls, "verify:"+level)
		return nil
	}
	eng.doctorAudit = func(*sql.DB) (DoctorPhysicalAudit, DoctorSnapshotAudit, error) {
		calls = append(calls, "audit")
		return DoctorPhysicalAudit{LogicalRefCountMismatches: 3}, DoctorSnapshotAudit{SnapshotFileRows: 4}, nil
	}

	result, err := eng.Doctor(context.Background(), DoctorRequest{VerifyLevel: "full"})
	if err != nil {
		t.Fatalf("Doctor: %v", err)
	}
	if want := []string{"recovery", "schema", "verify:full", "audit"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("stage order=%v want %v", calls, want)
	}
	if result.RecoveryStatus != "ok" || result.SchemaStatus != "ok" || result.VerifyStatus != "ok" || result.FailedStage != "" {
		t.Fatalf("statuses=%+v", result)
	}
	if result.Recovery.AbortedLogicalFiles != 2 || result.SchemaVersion != 8 || result.PhysicalAudit.LogicalRefCountMismatches != 3 || result.SnapshotAudit.SnapshotFileRows != 4 {
		t.Fatalf("neutral report=%+v", result)
	}
}

func TestDoctorShortCircuitsAtFirstFailedStage(t *testing.T) {
	tests := []struct {
		name      string
		failStage DoctorStage
		wantCode  ErrorCode
		wantCalls []string
	}{
		{"recovery", DoctorStageRecovery, ErrorRecoveryFailed, []string{"recovery"}},
		{"schema", DoctorStageSchema, ErrorOperationFailed, []string{"recovery", "schema"}},
		{"verify", DoctorStageVerify, ErrorVerificationFailed, []string{"recovery", "schema", "verify"}},
		{"audit", DoctorStageAudit, ErrorVerificationFailed, []string{"recovery", "schema", "verify", "audit"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng := newDoctorHookEngine(t)
			var calls []string
			eng.doctorRecover = func(context.Context) (RecoverResult, error) {
				calls = append(calls, "recovery")
				if tc.failStage == DoctorStageRecovery {
					return RecoverResult{}, errors.New("recovery fault")
				}
				return RecoverResult{}, nil
			}
			eng.doctorSchema = func(*sql.DB) (int64, error) {
				calls = append(calls, "schema")
				if tc.failStage == DoctorStageSchema {
					return 0, errors.New("schema fault")
				}
				return 8, nil
			}
			eng.doctorVerify = func(context.Context, string) error {
				calls = append(calls, "verify")
				if tc.failStage == DoctorStageVerify {
					return errors.New("verify fault")
				}
				return nil
			}
			eng.doctorAudit = func(*sql.DB) (DoctorPhysicalAudit, DoctorSnapshotAudit, error) {
				calls = append(calls, "audit")
				if tc.failStage == DoctorStageAudit {
					return DoctorPhysicalAudit{}, DoctorSnapshotAudit{}, errors.New("audit fault")
				}
				return DoctorPhysicalAudit{}, DoctorSnapshotAudit{}, nil
			}

			result, err := eng.Doctor(context.Background(), DoctorRequest{})
			if err == nil || !IsCode(err, tc.wantCode) || !strings.Contains(err.Error(), "doctor "+tc.name) {
				t.Fatalf("Doctor error=%v code=%q", err, CodeOf(err))
			}
			if result.FailedStage != tc.failStage || !reflect.DeepEqual(calls, tc.wantCalls) {
				t.Fatalf("result=%+v calls=%v want %v", result, calls, tc.wantCalls)
			}
		})
	}
}

func TestDoctorRejectsInvalidLevelBeforeCorrectiveRecovery(t *testing.T) {
	eng := newDoctorHookEngine(t)
	called := false
	eng.doctorRecover = func(context.Context) (RecoverResult, error) {
		called = true
		return RecoverResult{}, nil
	}
	_, err := eng.Doctor(context.Background(), DoctorRequest{VerifyLevel: "fast"})
	if !IsCode(err, ErrorInvalidArgument) || called {
		t.Fatalf("invalid level error=%v code=%q recovery_called=%v", err, CodeOf(err), called)
	}
}

func newDoctorHookEngine(t *testing.T) *DefaultEngine {
	t.Helper()
	eng, err := New(Config{DB: newEngineTestDB(t), ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng
}
