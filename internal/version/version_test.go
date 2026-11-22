package version

import (
	"runtime/debug"
	"testing"
)

func TestStringReturnsSemverFromConstants(t *testing.T) {
	if got, want := String(), "2.0.0-dev.0"; got != want {
		t.Fatalf("String() mismatch: got=%q want=%q", got, want)
	}
}

func TestChannelSeparatesPrereleases(t *testing.T) {
	for label, want := range map[string]string{"": "stable", "dev.0": "dev", "alpha.1": "alpha", "beta.2": "beta", "rc.1": "rc", "preview.1": "prerelease"} {
		if got := channel(label); got != want {
			t.Errorf("channel(%q) = %q, want %q", label, got, want)
		}
	}
	if Channel() != "dev" {
		t.Fatal("development build claims wrong channel")
	}
}

func TestBuildProvenanceDoesNotChangeVersion(t *testing.T) {
	before := String()
	for _, modified := range []string{"true", "false"} {
		got := buildInfo(&debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "fixture-revision"}, {Key: "vcs.modified", Value: modified}}})
		if got.Revision != "fixture-revision" || got.Modified == nil || *got.Modified != (modified == "true") {
			t.Fatalf("wrong provenance: %+v", got)
		}
	}
	if buildInfo(&debug.BuildInfo{}).Modified != nil || String() != before {
		t.Fatal("unknown provenance or version changed")
	}
}
