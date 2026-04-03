package listing

import (
	"strings"
	"testing"
)

func TestListFilesFailsWhenDBIsNil(t *testing.T) {
	_, err := ListFilesResultWithDB(nil, nil)
	if err == nil || !strings.Contains(err.Error(), "db connection is nil") {
		t.Fatalf("expected db-nil error contract, got: %v", err)
	}
}
