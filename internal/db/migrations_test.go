package db

import "testing"

func TestLoadPostgresAutoBootstrapEnabledReadsCurrentEnv(t *testing.T) {
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "false")
	if loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected auto-bootstrap to be disabled")
	}

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	if !loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected auto-bootstrap to be enabled after env change")
	}

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", " 'On' ")
	if !loadPostgresAutoBootstrapEnabled() {
		t.Fatal("expected quoted mixed-case truthy env value to be enabled")
	}
}
