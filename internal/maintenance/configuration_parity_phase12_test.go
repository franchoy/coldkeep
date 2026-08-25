package maintenance

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
)

func TestStatsBlockTargetMatchesStoreEffectiveResolver(t *testing.T) {
	dbconn := openStatsTestDB(t)
	containerResult, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES (?, 0, ?, 1, 0)`, "phase12-stats.bin", 64*1024*1024)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := containerResult.LastInsertId()
	if err != nil {
		t.Fatalf("container id: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'none', ?, ?, ?, 0, x'010203')`, int64(1<<20), int64(1<<20), containerID); err != nil {
		t.Fatalf("insert storage block: %v", err)
	}

	type envValue struct {
		set   bool
		value string
	}
	type tableCase struct {
		name      string
		newEnv    envValue
		legacyEnv envValue
		wantBytes int64
	}
	cases := []tableCase{
		{name: "both unset", wantBytes: 1 << 20},
		{name: "new 1", newEnv: envValue{true, "1"}, wantBytes: 1 << 20},
		{name: "new 2", newEnv: envValue{true, "2"}, wantBytes: 2 << 20},
		{name: "new 3", newEnv: envValue{true, "3"}, wantBytes: 3 << 20},
		{name: "new unsupported 4", newEnv: envValue{true, "4"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
		{name: "new zero", newEnv: envValue{true, "0"}, wantBytes: 1 << 20},
		{name: "new negative", newEnv: envValue{true, "-1"}, wantBytes: 1 << 20},
		{name: "new empty", newEnv: envValue{true, ""}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
		{name: "new numeric suffix", newEnv: envValue{true, "2MiB"}, wantBytes: 1 << 20},
		{name: "new overflow-like", newEnv: envValue{true, "9223372036854775808"}, wantBytes: 1 << 20},
		{name: "legacy 1", legacyEnv: envValue{true, "1"}, wantBytes: 1 << 20},
		{name: "legacy 2", legacyEnv: envValue{true, "2"}, wantBytes: 2 << 20},
		{name: "legacy 3", legacyEnv: envValue{true, "3"}, wantBytes: 3 << 20},
		{name: "legacy unsupported 4", legacyEnv: envValue{true, "4"}, wantBytes: 1 << 20},
		{name: "new valid wins", newEnv: envValue{true, "2"}, legacyEnv: envValue{true, "3"}, wantBytes: 2 << 20},
		{name: "new unsupported remains authoritative", newEnv: envValue{true, "4"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
		{name: "new malformed remains authoritative", newEnv: envValue{true, "invalid"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setStatsTargetEnvironment(t, "COLDKEEP_BLOCK_TARGET_SIZE_MB", tc.newEnv.set, tc.newEnv.value)
			setStatsTargetEnvironment(t, "COLDKEEP_PACKED_BLOCK_SIZE_MIB", tc.legacyEnv.set, tc.legacyEnv.value)

			var logs bytes.Buffer
			oldWriter := log.Writer()
			log.SetOutput(&logs)
			t.Cleanup(func() { log.SetOutput(oldWriter) })

			stats, err := CollectBlockStats(context.Background(), dbconn)
			if err != nil {
				t.Fatalf("CollectBlockStats: %v", err)
			}
			wantFillRatio := float64(1<<20) / float64(tc.wantBytes)
			if stats.FillRatio != wantFillRatio {
				t.Fatalf("Stats FillRatio = %v, want %v from Store-compatible %d-byte target", stats.FillRatio, wantFillRatio, tc.wantBytes)
			}
			if logs.Len() != 0 {
				t.Fatalf("Stats emitted Store warning output: %q", logs.String())
			}
		})
	}
}

func setStatsTargetEnvironment(t *testing.T, key string, set bool, value string) {
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
