package graph

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"
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

func TestTraverseVisitsAllNodesOnce(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "1", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}

	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, _ := lfRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", logicalFileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-a", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	chunkAID, _ := chunkARes.LastInsertId()
	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-b", 60, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	chunkBID, _ := chunkBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, logicalFileID, chunkAID, 0, logicalFileID, chunkBID, 1); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr.bin", 200, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := ctrRes.LastInsertId()
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, 'plain', 1, ?, ?, ?, ?)`,
		chunkAID, 40, 40, containerID, 0,
	); err != nil {
		t.Fatalf("insert block a: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, 'plain', 1, ?, ?, ?, ?)`,
		chunkBID, 60, 60, containerID, 64,
	); err != nil {
		t.Fatalf("insert block b: %v", err)
	}

	visited := make(map[NodeID]int)
	err = svc.Traverse(context.Background(), []NodeID{{Type: EntitySnapshot, ID: 1}}, func(n NodeID) error {
		visited[n]++
		return nil
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}

	for node, count := range visited {
		if count != 1 {
			t.Fatalf("node %+v visited %d times", node, count)
		}
	}

	typesSeen := make(map[EntityType]struct{})
	for node := range visited {
		typesSeen[node.Type] = struct{}{}
	}
	for _, typ := range []EntityType{EntitySnapshot, EntityLogicalFile, EntityChunk, EntityContainer} {
		if _, ok := typesSeen[typ]; !ok {
			t.Fatalf("expected type %s in traversal, seen=%v", typ, typesSeen)
		}
	}
}

func TestTraverseDeterministicOrder(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "1", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	firstLogicalID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"b.txt", 120, "lf-b", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	secondLogicalID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/b.txt"); err != nil {
		t.Fatalf("insert second snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", secondLogicalID, "1", "docs/b.txt", firstLogicalID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"chunk-a", 40, "COMPLETED", 1, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	firstChunkID, _ := chunkARes.LastInsertId()
	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"chunk-b", 50, "COMPLETED", 1, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	secondChunkID, _ := chunkBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, firstLogicalID, secondChunkID, 1, secondLogicalID, firstChunkID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	runTraversal := func() []NodeID {
		visited := make([]NodeID, 0)
		err := svc.Traverse(context.Background(), []NodeID{{Type: EntitySnapshot, ID: 1}}, func(n NodeID) error {
			visited = append(visited, n)
			return nil
		})
		if err != nil {
			t.Fatalf("Traverse: %v", err)
		}
		return visited
	}

	result1 := runTraversal()
	result2 := runTraversal()
	if !reflect.DeepEqual(result1, result2) {
		t.Fatalf("expected deterministic traversal order, got result1=%+v result2=%+v", result1, result2)
	}
}

func TestTraverseNoCyclesInfiniteLoops(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert paths: %v", err)
	}
	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "shared.txt", 100, "lf-shared", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	lfID, _ := lfRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfID, "2", "docs/b.txt", lfID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "shared-chunk", 40, "COMPLETED", 2, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, lfID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	visited := make([]NodeID, 0)
	err = svc.Traverse(ctx, []NodeID{{Type: EntitySnapshot, ID: 1}, {Type: EntitySnapshot, ID: 2}}, func(n NodeID) error {
		visited = append(visited, n)
		return nil
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	if len(visited) == 0 {
		t.Fatal("expected visited nodes")
	}

	keyed := make(map[NodeID]int)
	for _, n := range visited {
		keyed[n]++
	}
	for n, c := range keyed {
		if c != 1 {
			t.Fatalf("node %+v visited %d times, expected dedup visitation", n, c)
		}
	}

	kinds := make([]string, 0, len(keyed))
	for n := range keyed {
		kinds = append(kinds, string(n.Type))
	}
	sort.Strings(kinds)
	if len(kinds) < 3 {
		t.Fatalf("expected traversal across repeated references, kinds=%v", kinds)
	}
}

func TestTraverseWithOptionsEmitsTraceEvents(t *testing.T) {
	svc := NewService(nil)
	start := []NodeID{{Type: EntitySnapshot, ID: 1}, {Type: EntitySnapshot, ID: 1}}

	steps := make([]string, 0)
	skipped := 0
	err := svc.TraverseWithOptions(context.Background(), start, func(NodeID) error {
		return nil
	}, TraversalOptions{
		Trace: func(event TraceEvent) {
			steps = append(steps, event.Step)
			if event.Step == "graph.node.skip_already_visited" {
				skipped++
			}
		},
	})
	if err != nil {
		t.Fatalf("TraverseWithOptions: %v", err)
	}

	if len(steps) == 0 {
		t.Fatal("expected trace events")
	}
	if steps[0] != "graph.traverse.start" {
		t.Fatalf("first trace step = %q, want graph.traverse.start", steps[0])
	}
	if steps[len(steps)-1] != "graph.traverse.complete" {
		t.Fatalf("last trace step = %q, want graph.traverse.complete", steps[len(steps)-1])
	}

	wantPresent := []string{
		"graph.node.visit",
		"graph.edge.load",
		"graph.edge.loaded",
		"graph.node.skip_already_visited",
	}
	for _, want := range wantPresent {
		found := false
		for _, got := range steps {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing trace step %q in %v", want, steps)
		}
	}

	if skipped != 1 {
		t.Fatalf("skip events = %d, want 1", skipped)
	}
}

func TestTraverseWithOptionsNilTraceIsNoop(t *testing.T) {
	svc := NewService(nil)
	err := svc.TraverseWithOptions(context.Background(), []NodeID{{Type: EntitySnapshot, ID: 1}}, func(NodeID) error {
		return nil
	}, TraversalOptions{})
	if err != nil {
		t.Fatalf("TraverseWithOptions with nil trace should not fail: %v", err)
	}
}
