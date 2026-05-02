package utils_print

import (
	"errors"
	"testing"
)

func TestAppendToErrorListHonorsCap(t *testing.T) {
	list := make([]error, 0, MaxErrorsToPrint)
	for i := 0; i < MaxErrorsToPrint+5; i++ {
		list = AppendToErrorList(list, errors.New("x"))
	}
	if got := len(list); got != MaxErrorsToPrint {
		t.Fatalf("error list length mismatch: got=%d want=%d", got, MaxErrorsToPrint)
	}
}
