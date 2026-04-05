package container

import "testing"

func TestSetContainerMaxSizeOverridesGlobalValue(t *testing.T) {
	orig := GetContainerMaxSize()
	t.Cleanup(func() { SetContainerMaxSize(orig) })

	SetContainerMaxSize(123456)
	if got := GetContainerMaxSize(); got != 123456 {
		t.Fatalf("expected overridden container max size 123456, got %d", got)
	}
}

func TestSetContainerMaxSizeAllowsZeroAndNegativeValues(t *testing.T) {
	orig := GetContainerMaxSize()
	t.Cleanup(func() { SetContainerMaxSize(orig) })

	SetContainerMaxSize(0)
	if got := GetContainerMaxSize(); got != 0 {
		t.Fatalf("expected container max size 0, got %d", got)
	}

	SetContainerMaxSize(-1)
	if got := GetContainerMaxSize(); got != -1 {
		t.Fatalf("expected container max size -1, got %d", got)
	}
}
