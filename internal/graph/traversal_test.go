package graph

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTraverseVisitsStartNodesInOrderWithoutDuplicates(t *testing.T) {
	svc := NewService(nil)

	a := NodeID{Type: EntitySnapshot, ID: 1}
	b := NodeID{Type: EntityLogicalFile, ID: 2}
	c := NodeID{Type: EntityChunk, ID: 3}

	start := []NodeID{a, b, a, c, b}
	visited := make([]NodeID, 0, 3)

	err := svc.Traverse(context.Background(), start, func(n NodeID) error {
		visited = append(visited, n)
		return nil
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}

	want := []NodeID{a, b, c}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("unexpected visited order: got=%+v want=%+v", visited, want)
	}
}

func TestTraverseReturnsVisitError(t *testing.T) {
	svc := NewService(nil)
	boom := errors.New("boom")

	err := svc.Traverse(context.Background(), []NodeID{{Type: EntitySnapshot, ID: 1}}, func(_ NodeID) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected visit error, got %v", err)
	}
}

func TestTraverseRespectsCanceledContext(t *testing.T) {
	svc := NewService(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.Traverse(ctx, []NodeID{{Type: EntitySnapshot, ID: 1}}, func(_ NodeID) error {
		t.Fatal("visit should not be called for canceled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}
