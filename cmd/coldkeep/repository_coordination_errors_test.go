package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/coordination"
)

func TestStableCoordinationErrorMapping(t *testing.T) {
	unexpectedIO := errors.New("unexpected native I/O")
	tests := []struct {
		name        string
		err         error
		wantCause   error
		wantCode    string
		wantExit    int
		wantMessage string
	}{
		{
			name:        "busy wrapped",
			err:         fmt.Errorf("acquire: %w", coordination.ErrRepositoryBusy),
			wantCause:   coordination.ErrRepositoryBusy,
			wantCode:    publicCodeRepositoryBusy,
			wantExit:    exitGeneral,
			wantMessage: "repository is busy",
		},
		{
			name:        "unsupported wrapped",
			err:         fmt.Errorf("acquire: %w", coordination.ErrRepositoryLockUnsupported),
			wantCause:   coordination.ErrRepositoryLockUnsupported,
			wantCode:    publicCodeRepositoryLockUnsupported,
			wantExit:    exitGeneral,
			wantMessage: "repository locking is unsupported",
		},
		{
			name:        "identity invalid wrapped",
			err:         fmt.Errorf("resolve: %w", coordination.ErrRepositoryIdentityInvalid),
			wantCause:   coordination.ErrRepositoryIdentityInvalid,
			wantCode:    "INVALID_ARGUMENT",
			wantExit:    exitUsage,
			wantMessage: "repository identity is invalid",
		},
		{
			name:        "nested acquisition",
			err:         coordination.ErrNestedRepositoryAcquisition,
			wantCause:   coordination.ErrNestedRepositoryAcquisition,
			wantCode:    "INTERNAL",
			wantExit:    exitGeneral,
			wantMessage: "repository coordination failed",
		},
		{
			name:        "wrapped permission",
			err:         fmt.Errorf("open repository lock: %w", os.ErrPermission),
			wantCause:   os.ErrPermission,
			wantCode:    publicCodePermissionDenied,
			wantExit:    exitGeneral,
			wantMessage: "permission denied",
		},
		{
			name:        "context canceled",
			err:         fmt.Errorf("acquire: %w", context.Canceled),
			wantCause:   context.Canceled,
			wantCode:    publicCodeCanceled,
			wantExit:    exitGeneral,
			wantMessage: "operation canceled",
		},
		{
			name:        "context deadline",
			err:         fmt.Errorf("acquire: %w", context.DeadlineExceeded),
			wantCause:   context.DeadlineExceeded,
			wantCode:    publicCodeDeadlineExceeded,
			wantExit:    exitGeneral,
			wantMessage: "operation deadline exceeded",
		},
		{
			name:        "unexpected coordination I/O",
			err:         markRepositoryCoordinationFailure(unexpectedIO),
			wantCause:   unexpectedIO,
			wantCode:    "INTERNAL",
			wantExit:    exitGeneral,
			wantMessage: "repository coordination failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stableErr := stableCLIError(test.err)
			if !errors.Is(stableErr, test.wantCause) {
				t.Fatalf("stable error=%v does not preserve cause %v", stableErr, test.wantCause)
			}
			if got := classifyExitCode(stableErr); got != test.wantExit {
				t.Fatalf("exit=%d want=%d", got, test.wantExit)
			}
			if got := publicErrorCode(stableErr, test.wantExit); got != test.wantCode {
				t.Fatalf("public code=%q want=%q", got, test.wantCode)
			}
			if got := stableErr.Error(); got != test.wantMessage {
				t.Fatalf("message=%q want=%q", got, test.wantMessage)
			}
		})
	}
}

func TestStableCoordinationJoinedErrorPreservesOperationPrecedenceAndCauses(t *testing.T) {
	operationCause := errors.New("invalid operation input")
	operationErr := observabilityWrappedError(exitUsage, "INVALID_ARGUMENT", "invalid request", operationCause)
	releaseCause := errors.New("native release failure")
	releaseErr := markRepositoryCoordinationFailure(releaseCause)
	joinedErr := errors.Join(operationErr, releaseErr)

	stableErr := stableCLIError(joinedErr)
	if !errors.Is(stableErr, operationCause) || !errors.Is(stableErr, releaseCause) {
		t.Fatalf("stable error lost joined causes: %v", stableErr)
	}
	if got := classifyExitCode(stableErr); got != exitUsage {
		t.Fatalf("exit=%d want=%d", got, exitUsage)
	}
	if got := publicErrorCode(stableErr, exitUsage); got != "INVALID_ARGUMENT" {
		t.Fatalf("public code=%q want=INVALID_ARGUMENT", got)
	}
}

func TestStableCoordinationJoinedSentinelUsesFirstCause(t *testing.T) {
	secondaryCause := errors.New("secondary acquisition detail")
	joinedErr := errors.Join(
		fmt.Errorf("acquire: %w", coordination.ErrRepositoryBusy),
		secondaryCause,
	)

	stableErr := stableCLIError(joinedErr)
	if !errors.Is(stableErr, coordination.ErrRepositoryBusy) || !errors.Is(stableErr, secondaryCause) {
		t.Fatalf("stable error lost joined causes: %v", stableErr)
	}
	if got := classifyExitCode(stableErr); got != exitGeneral {
		t.Fatalf("exit=%d want=%d", got, exitGeneral)
	}
	if got := publicErrorCode(stableErr, exitGeneral); got != publicCodeRepositoryBusy {
		t.Fatalf("public code=%q want=%q", got, publicCodeRepositoryBusy)
	}
}

func TestCoordinationJSONErrorRenderingUsesStableCodesAndSafeMessages(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode string
	}{
		{
			name:     "busy",
			err:      fmt.Errorf("lock /secret/repository/.coldkeep-control/repository.lock: %w", coordination.ErrRepositoryBusy),
			wantCode: publicCodeRepositoryBusy,
		},
		{
			name:     "unsupported",
			err:      fmt.Errorf("lock /secret/repository/.coldkeep-control/repository.lock: %w", coordination.ErrRepositoryLockUnsupported),
			wantCode: publicCodeRepositoryLockUnsupported,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var code int
			stderr := captureStderr(t, func() {
				code = printCLIError(test.err, outputModeJSON)
			})
			if code != exitGeneral {
				t.Fatalf("exit=%d want=%d", code, exitGeneral)
			}
			if strings.Contains(stderr, "/secret/repository") {
				t.Fatalf("JSON error leaked repository path: %s", stderr)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &payload); err != nil {
				t.Fatalf("decode JSON error: %v output=%q", err, stderr)
			}
			errorNode, ok := payload["error"].(map[string]any)
			if !ok {
				t.Fatalf("error node=%T want object", payload["error"])
			}
			if got, _ := errorNode["code"].(string); got != test.wantCode {
				t.Fatalf("code=%q want=%q payload=%v", got, test.wantCode, payload)
			}
		})
	}
}

func TestCoordinationTextErrorRenderingUsesSafeMessage(t *testing.T) {
	err := fmt.Errorf("lock /secret/repository/.coldkeep-control/repository.lock: %w", coordination.ErrRepositoryBusy)
	var code int
	stderr := captureStderr(t, func() {
		code = printCLIError(err, outputModeText)
	})
	if code != exitGeneral {
		t.Fatalf("exit=%d want=%d", code, exitGeneral)
	}
	if strings.Contains(stderr, "/secret/repository") {
		t.Fatalf("text error leaked repository path: %s", stderr)
	}
	if !strings.Contains(stderr, "repository is busy") {
		t.Fatalf("text error=%q does not contain stable busy message", stderr)
	}
}

func TestRunCLICoordinationAcquisitionErrorsUseStableSurfaceAndShortCircuit(t *testing.T) {
	unexpectedIO := errors.New("native coordination I/O failure")
	tests := []struct {
		name     string
		err      error
		wantCode string
		wantExit int
	}{
		{name: "busy", err: fmt.Errorf("acquire: %w", coordination.ErrRepositoryBusy), wantCode: publicCodeRepositoryBusy, wantExit: exitGeneral},
		{name: "unsupported", err: fmt.Errorf("acquire: %w", coordination.ErrRepositoryLockUnsupported), wantCode: publicCodeRepositoryLockUnsupported, wantExit: exitGeneral},
		{name: "identity invalid", err: fmt.Errorf("resolve: %w", coordination.ErrRepositoryIdentityInvalid), wantCode: "INVALID_ARGUMENT", wantExit: exitUsage},
		{name: "nested", err: coordination.ErrNestedRepositoryAcquisition, wantCode: "INTERNAL", wantExit: exitGeneral},
		{name: "permission", err: fmt.Errorf("open lock: %w", os.ErrPermission), wantCode: publicCodePermissionDenied, wantExit: exitGeneral},
		{name: "canceled", err: context.Canceled, wantCode: publicCodeCanceled, wantExit: exitGeneral},
		{name: "deadline", err: context.DeadlineExceeded, wantCode: publicCodeDeadlineExceeded, wantExit: exitGeneral},
		{name: "unexpected I/O", err: unexpectedIO, wantCode: "INTERNAL", wantExit: exitGeneral},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := &cliLifecycleTrace{}
			runtime := newTestCLIRuntime(t, trace)
			runtime.newCoordinator = func() coordination.Coordinator {
				return &fakeCLICoordinator{
					acquireFn: func(context.Context, coordination.Identity, coordination.Request) (coordination.Lease, error) {
						trace.add("lease acquire")
						return nil, test.err
					},
				}
			}

			stdout, stderr, code := captureRuntimeCLI(t, []string{"stats", "--output", "json"}, runtime)
			if code != test.wantExit {
				t.Fatalf("exit=%d want=%d stderr=%q", code, test.wantExit, stderr)
			}
			if strings.TrimSpace(stdout) != "" {
				t.Fatalf("stdout=%q want empty", stdout)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &payload); err != nil {
				t.Fatalf("decode JSON error: %v output=%q", err, stderr)
			}
			errorNode, ok := payload["error"].(map[string]any)
			if !ok {
				t.Fatalf("error node=%T want object", payload["error"])
			}
			if got, _ := errorNode["code"].(string); got != test.wantCode {
				t.Fatalf("code=%q want=%q payload=%v", got, test.wantCode, payload)
			}
			trace.require(t, []string{"lease acquire"})
		})
	}
}
