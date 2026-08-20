package application

import (
	"context"
	"testing"
)

func TestNilSessionIsSafe(t *testing.T) {
	var session *Session
	if session.Engine() != nil {
		t.Fatal("nil session returned an engine")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("nil session close: %v", err)
	}
	ctx, cancel := session.OperationContext(context.Background())
	defer cancel()
	if ctx == nil {
		t.Fatal("nil session operation context is nil")
	}
}
