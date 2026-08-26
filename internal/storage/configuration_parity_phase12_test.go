package storage

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func TestPackedBlockTargetPhase11StoreSemanticsTable(t *testing.T) {
	type envValue struct {
		set   bool
		value string
	}
	type tableCase struct {
		name        string
		newEnv      envValue
		legacyEnv   envValue
		wantBytes   int64
		wantWarning string
	}
	cases := []tableCase{
		{name: "both unset", wantBytes: 1 << 20},
		{name: "new 1", newEnv: envValue{true, "1"}, wantBytes: 1 << 20},
		{name: "new 2", newEnv: envValue{true, "2"}, wantBytes: 2 << 20},
		{name: "new 3", newEnv: envValue{true, "3"}, wantBytes: 3 << 20},
		{name: "new unsupported 4", newEnv: envValue{true, "4"}, wantBytes: 1 << 20, wantWarning: "unsupported packed block target size mb=4"},
		{name: "new zero", newEnv: envValue{true, "0"}, wantBytes: 1 << 20, wantWarning: "invalid packed block target size mb=0"},
		{name: "new negative", newEnv: envValue{true, "-1"}, wantBytes: 1 << 20, wantWarning: "invalid packed block target size mb=-1"},
		{name: "new empty remains authoritative", newEnv: envValue{true, ""}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
		{name: "new numeric suffix uses helper fallback", newEnv: envValue{true, "2MiB"}, wantBytes: 1 << 20},
		{name: "new overflow-like uses helper fallback", newEnv: envValue{true, "9223372036854775808"}, wantBytes: 1 << 20},
		{name: "legacy 1", legacyEnv: envValue{true, "1"}, wantBytes: 1 << 20},
		{name: "legacy 2", legacyEnv: envValue{true, "2"}, wantBytes: 2 << 20},
		{name: "legacy 3", legacyEnv: envValue{true, "3"}, wantBytes: 3 << 20},
		{name: "legacy unsupported 4", legacyEnv: envValue{true, "4"}, wantBytes: 1 << 20, wantWarning: "unsupported packed block target size mb=4"},
		{name: "new valid wins", newEnv: envValue{true, "2"}, legacyEnv: envValue{true, "3"}, wantBytes: 2 << 20},
		{name: "new unsupported does not fall through", newEnv: envValue{true, "4"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20, wantWarning: "unsupported packed block target size mb=4"},
		{name: "new malformed does not fall through", newEnv: envValue{true, "invalid"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPackedBlockTargetEnvironment(t, "COLDKEEP_BLOCK_TARGET_SIZE_MB", tc.newEnv.set, tc.newEnv.value)
			setPackedBlockTargetEnvironment(t, "COLDKEEP_PACKED_BLOCK_SIZE_MIB", tc.legacyEnv.set, tc.legacyEnv.value)

			var logs bytes.Buffer
			oldWriter := log.Writer()
			log.SetOutput(&logs)
			t.Cleanup(func() { log.SetOutput(oldWriter) })

			if got := packedBlockTargetSizeBytesFromEnv(); got != tc.wantBytes {
				t.Fatalf("target bytes = %d, want %d", got, tc.wantBytes)
			}
			output := logs.String()
			if tc.wantWarning == "" {
				if output != "" {
					t.Fatalf("unexpected warning output: %q", output)
				}
				return
			}
			if !strings.Contains(output, tc.wantWarning) {
				t.Fatalf("warning output = %q, want substring %q", output, tc.wantWarning)
			}
			if lines := strings.Count(strings.TrimSpace(output), "\n") + 1; lines != 1 {
				t.Fatalf("warning line count = %d, want 1: %q", lines, output)
			}
		})
	}
}

func TestPackedBlockTargetResolverDrivesBuilderFlushBoundaries(t *testing.T) {
	cases := []struct {
		name      string
		value     string
		wantBytes int64
	}{
		{name: "one MiB", value: "1", wantBytes: 1 << 20},
		{name: "two MiB", value: "2", wantBytes: 2 << 20},
		{name: "three MiB", value: "3", wantBytes: 3 << 20},
		{name: "unsupported falls back", value: "4", wantBytes: 1 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPackedBlockTargetEnvironment(t, "COLDKEEP_BLOCK_TARGET_SIZE_MB", true, tc.value)
			setPackedBlockTargetEnvironment(t, "COLDKEEP_PACKED_BLOCK_SIZE_MIB", false, "")
			target := packedBlockTargetSizeBytesFromEnv()
			if target != tc.wantBytes {
				t.Fatalf("target bytes = %d, want %d", target, tc.wantBytes)
			}
			builder := blocks.NewBlockBuilder(target)
			payload := make([]byte, target-1)
			if err := builder.Add(blocks.PendingChunk{ChunkID: 1, Data: payload, Size: int64(len(payload))}); err != nil {
				t.Fatalf("add first chunk: %v", err)
			}
			if builder.ShouldFlushBeforeAdd(1) {
				t.Fatal("builder flushed before exactly reaching effective target")
			}
			if !builder.ShouldFlushBeforeAdd(2) {
				t.Fatal("builder did not flush before exceeding effective target")
			}
		})
	}
}

func setPackedBlockTargetEnvironment(t *testing.T, key string, set bool, value string) {
	t.Helper()
	old, hadOld := os.LookupEnv(key)
	t.Cleanup(func() {
		if hadOld {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	if !set {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		return
	}
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}
