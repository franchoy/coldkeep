// Package transforms defines the composable transform pipeline used during
// block persistence and retrieval. Each Transform operates on raw bytes,
// making stages independently testable and composable without side effects.
//
// # Three-layer storage model
//
// Every persisted block passes through three conceptually distinct layers:
//
//	┌─────────────────────────────────────────────────────────────────┐
//	│ Layer 1 — Logical block                                         │
//	│   canonical encoded plaintext block bytes                       │
//	│   → identity is block_hash = sha256(logical block bytes)        │
//	│   → this is the dedup key and the restore integrity anchor      │
//	├─────────────────────────────────────────────────────────────────┤
//	│ Layer 2 — Transformed payload                                   │
//	│   output of the transform pipeline (e.g. compressed, encrypted) │
//	│   → future: compressed_hash = sha256(post-compression bytes)    │
//	│   → not a dedup key; diagnostic / repair checkpoint only        │
//	├─────────────────────────────────────────────────────────────────┤
//	│ Layer 3 — Persisted payload                                     │
//	│   exact bytes written to the container file                     │
//	│   → future: physical_hash = sha256(persisted bytes)             │
//	│   → corruption detection, transfer validation                   │
//	└─────────────────────────────────────────────────────────────────┘
//
// # Hash semantics
//
// block_hash (Layer 1) is computed BEFORE any transform is applied.
// It is the canonical logical identity of a block and never changes
// regardless of which transforms are active. Dedup, GC, and restore
// all operate against this hash.
//
// Future hashes (compressed_hash, physical_hash) will be computed at
// their respective layer boundaries and stored in separate columns.
// They carry no dedup semantics.
//
// # Transform ordering invariant (frozen v1.9)
//
// Write path contract:
//
//	logical encode
//	→ logical hash
//	→ compression
//	→ compressed hash
//	→ encryption
//	→ physical hash
//	→ persist
//
// Read path contract (strict inverse):
//
//	read payload
//	→ physical hash verify
//	→ decrypt
//	→ compressed hash verify
//	→ decompress
//	→ logical hash verify
//	→ decode logical block
//
// TransformPipeline enforces forward encode and reverse decode symmetry for
// transform stages. Changing stage order changes repository semantics and is
// intentionally considered a new major storage contract.
//
// # Future compression insertion point
//
// When compression is introduced (Phase 3), it will be inserted as transform[0]
// (before encryption), so the write path becomes:
//
//	logical block → compress → encrypt → persisted payload
//
// and the read path becomes:
//
//	persisted payload → decrypt → decompress → logical block
//
// No changes to block_hash semantics or dedup logic are required.
package transforms

import "fmt"

// Transform is a single composable stage in the storage transform pipeline.
// Implementations must be stateless with respect to individual Encode/Decode calls;
// any key material or configuration is held in the struct fields.
//
// Encode transforms plaintext bytes into stored bytes for a single stage.
// Decode reverses the transformation, recovering the original bytes.
//
// Both directions must be deterministic for the same input when the transform
// is non-random (e.g. compression). For transforms that introduce randomness
// (e.g. encryption with a random nonce), the stored bytes carry the necessary
// metadata to reverse the operation without any external state.
type Transform interface {
	// Name returns a stable, lowercase identifier for this transform stage.
	// Used for metadata, diagnostics, and future capability negotiation.
	Name() string

	// Encode transforms input bytes into the stored representation.
	Encode(input []byte) ([]byte, error)

	// Decode reverses the transformation, recovering the original bytes.
	Decode(input []byte) ([]byte, error)
}

// TransformPipeline executes an ordered sequence of Transform stages.
//
// Encode applies stages in forward order:
//
//	input → stage[0] → stage[1] → ... → stage[n-1] → output
//
// Decode applies stages in reverse order:
//
//	input → stage[n-1] → ... → stage[1] → stage[0] → output
//
// An empty pipeline is valid and behaves as an identity transform.
type TransformPipeline struct {
	stages []Transform
}

// NewTransformPipeline returns a pipeline that applies the given stages in order.
func NewTransformPipeline(stages ...Transform) *TransformPipeline {
	s := make([]Transform, len(stages))
	copy(s, stages)
	return &TransformPipeline{stages: s}
}

// Len returns the number of stages in the pipeline.
func (p *TransformPipeline) Len() int { return len(p.stages) }

// Encode applies each stage in forward order.
func (p *TransformPipeline) Encode(input []byte) ([]byte, error) {
	current := input
	for i, stage := range p.stages {
		out, err := stage.Encode(current)
		if err != nil {
			return nil, fmt.Errorf("pipeline encode stage %d (%s): %w", i, stage.Name(), err)
		}
		current = out
	}
	return current, nil
}

// Decode applies each stage in reverse order.
func (p *TransformPipeline) Decode(input []byte) ([]byte, error) {
	current := input
	for i := len(p.stages) - 1; i >= 0; i-- {
		stage := p.stages[i]
		out, err := stage.Decode(current)
		if err != nil {
			return nil, fmt.Errorf("pipeline decode stage %d (%s): %w", i, stage.Name(), err)
		}
		current = out
	}
	return current, nil
}
