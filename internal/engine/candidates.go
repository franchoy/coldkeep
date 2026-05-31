package engine

// Mutating operation candidates — inactive in v1.11.0 Phase 7.
//
// These request and result types define the future mutating operation contract
// for the engine facade. They are not part of the active Engine interface in
// v1.11.0 and must not be routed from CLI until explicit later phases.
//
// Safety invariants that any future activating phase must preserve:
//   - GC must never delete reachable data.
//   - Restore must never write outside the intended destination.
//   - Recovery must not legitimize corrupt mappings.
//   - Snapshot operations must preserve immutability and retention semantics.
//   - Transaction, locking, validation, and safety behavior must remain
//     consistent with the existing implementation.

// StoreRequest is a candidate request for a future Store operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type StoreRequest struct {
	SourcePath string
	Tags       []string
}

// StoreResult is a candidate result for a future Store operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type StoreResult struct {
	FileID int64
}

// RestoreRequest is a candidate request for a future Restore operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: Restore must never write outside the intended destination.
type RestoreRequest struct {
	FileID     int64
	OutputPath string
}

// RestoreResult is a candidate result for a future Restore operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RestoreResult struct{}

// RemoveRequest is a candidate request for a future Remove operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RemoveRequest struct {
	FileID int64
	Force  bool
}

// RemoveResult is a candidate result for a future Remove operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RemoveResult struct{}

// SnapshotCreateRequest is a candidate request for a future SnapshotCreate operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: Snapshot operations must preserve immutability and retention semantics.
type SnapshotCreateRequest struct {
	Label string
	Tags  []string
}

// SnapshotCreateResult is a candidate result for a future SnapshotCreate operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type SnapshotCreateResult struct {
	SnapshotID string
}

// SnapshotRestoreRequest is a candidate request for a future SnapshotRestore operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: Restore must never write outside the intended destination.
type SnapshotRestoreRequest struct {
	SnapshotID string
	OutputPath string
}

// SnapshotRestoreResult is a candidate result for a future SnapshotRestore operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type SnapshotRestoreResult struct{}

// SnapshotDeleteRequest is a candidate request for a future SnapshotDelete operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: Snapshot operations must preserve immutability and retention semantics.
type SnapshotDeleteRequest struct {
	SnapshotID string
	Force      bool
}

// SnapshotDeleteResult is a candidate result for a future SnapshotDelete operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type SnapshotDeleteResult struct{}

// GarbageCollectRequest is a candidate request for a future GarbageCollect operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: GC must never delete reachable data.
type GarbageCollectRequest struct {
	DryRun  bool
	Workers int
}

// GarbageCollectResult is a candidate result for a future GarbageCollect operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type GarbageCollectResult struct {
	Collected int64
}

// RepairRequest is a candidate request for a future Repair operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RepairRequest struct {
	DryRun bool
	Limit  int
}

// RepairResult is a candidate result for a future Repair operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RepairResult struct{}

// RecoverRequest is a candidate request for a future Recover operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
//
// Safety invariant: Recovery must not legitimize corrupt mappings.
type RecoverRequest struct {
	SnapshotID string
	OutputPath string
	Force      bool
}

// RecoverResult is a candidate result for a future Recover operation.
// Mutating operation candidate for v1.12+. Not part of the active v1.11 Engine interface.
type RecoverResult struct{}
