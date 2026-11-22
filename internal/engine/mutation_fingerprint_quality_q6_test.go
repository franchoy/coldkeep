package engine_test

import "testing"

func TestMutationBackendValueFingerprintFixedOutputs(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"binary", []byte{0x00, 0x0f, 0xa0, 0xff}, "000fa0ff"},
		{"empty_bytes", []byte{}, ""},
		{"nil_bytes", []byte(nil), ""},
		{"string", "A|b", "A|b"},
		{"integer", int64(-42), "-42"},
		{"float", 1.25, "1.25"},
		{"true", true, "true"},
		{"false", false, "false"},
		{"nil", nil, "<nil>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mutationBackendValueFingerprint(tc.value); got != tc.want {
				t.Fatalf("fingerprint = %q, want %q", got, tc.want)
			}
		})
	}
}
