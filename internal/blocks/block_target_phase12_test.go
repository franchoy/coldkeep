package blocks

import (
	"os"
	"testing"
)

func TestResolvePackedBlockTargetPreservesStoreSemantics(t *testing.T) {
	type envValue struct {
		set   bool
		value string
	}
	type tableCase struct {
		name        string
		newEnv      envValue
		legacyEnv   envValue
		wantBytes   int64
		wantMB      int64
		wantWarning PackedBlockTargetWarning
		wantSource  string
	}
	cases := []tableCase{
		{name: "both unset", wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_PACKED_BLOCK_SIZE_MIB"},
		{name: "new 1", newEnv: envValue{true, "1"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new 2", newEnv: envValue{true, "2"}, wantBytes: 2 << 20, wantMB: 2, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new 3", newEnv: envValue{true, "3"}, wantBytes: 3 << 20, wantMB: 3, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new 4", newEnv: envValue{true, "4"}, wantBytes: 1 << 20, wantMB: 4, wantWarning: PackedBlockTargetWarningUnsupported, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new zero", newEnv: envValue{true, "0"}, wantBytes: 1 << 20, wantMB: 0, wantWarning: PackedBlockTargetWarningInvalid, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new negative", newEnv: envValue{true, "-1"}, wantBytes: 1 << 20, wantMB: -1, wantWarning: PackedBlockTargetWarningInvalid, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new empty", newEnv: envValue{true, ""}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "numeric suffix follows helper fallback", newEnv: envValue{true, "2MiB"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "overflow-like follows helper fallback", newEnv: envValue{true, "9223372036854775808"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "legacy 1", legacyEnv: envValue{true, "1"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_PACKED_BLOCK_SIZE_MIB"},
		{name: "legacy 2", legacyEnv: envValue{true, "2"}, wantBytes: 2 << 20, wantMB: 2, wantSource: "COLDKEEP_PACKED_BLOCK_SIZE_MIB"},
		{name: "legacy 3", legacyEnv: envValue{true, "3"}, wantBytes: 3 << 20, wantMB: 3, wantSource: "COLDKEEP_PACKED_BLOCK_SIZE_MIB"},
		{name: "legacy 4", legacyEnv: envValue{true, "4"}, wantBytes: 1 << 20, wantMB: 4, wantWarning: PackedBlockTargetWarningUnsupported, wantSource: "COLDKEEP_PACKED_BLOCK_SIZE_MIB"},
		{name: "new valid wins", newEnv: envValue{true, "2"}, legacyEnv: envValue{true, "3"}, wantBytes: 2 << 20, wantMB: 2, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new unsupported does not fall through", newEnv: envValue{true, "4"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20, wantMB: 4, wantWarning: PackedBlockTargetWarningUnsupported, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
		{name: "new malformed does not fall through", newEnv: envValue{true, "invalid"}, legacyEnv: envValue{true, "2"}, wantBytes: 1 << 20, wantMB: 1, wantSource: "COLDKEEP_BLOCK_TARGET_SIZE_MB"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBlockTargetEnv(t, "COLDKEEP_BLOCK_TARGET_SIZE_MB", tc.newEnv.set, tc.newEnv.value)
			setBlockTargetEnv(t, "COLDKEEP_PACKED_BLOCK_SIZE_MIB", tc.legacyEnv.set, tc.legacyEnv.value)
			got := ResolvePackedBlockTarget()
			if got.Bytes != tc.wantBytes || got.Megabytes != tc.wantMB || got.Warning != tc.wantWarning || got.Environment != tc.wantSource {
				t.Fatalf("resolution = %+v, want bytes=%d mb=%d warning=%d source=%q", got, tc.wantBytes, tc.wantMB, tc.wantWarning, tc.wantSource)
			}
		})
	}
}

func setBlockTargetEnv(t *testing.T, key string, set bool, value string) {
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
