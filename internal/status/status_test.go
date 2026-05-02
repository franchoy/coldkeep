package status

import "testing"

func TestLogicalFileStatusConstants(t *testing.T) {
	if LogicalFileCompleted != "COMPLETED" || LogicalFileProcessing != "PROCESSING" || LogicalFileAborted != "ABORTED" {
		t.Fatalf("unexpected logical status constants: completed=%q processing=%q aborted=%q", LogicalFileCompleted, LogicalFileProcessing, LogicalFileAborted)
	}
}

func TestChunkStatusConstants(t *testing.T) {
	if ChunkCompleted != "COMPLETED" || ChunkProcessing != "PROCESSING" || ChunkAborted != "ABORTED" {
		t.Fatalf("unexpected chunk status constants: completed=%q processing=%q aborted=%q", ChunkCompleted, ChunkProcessing, ChunkAborted)
	}
}
