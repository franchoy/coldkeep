package verify

import (
	"context"
	"sync"
)

type verificationLayout string

const (
	verificationLayoutLegacy verificationLayout = "legacy"
	verificationLayoutPacked verificationLayout = "packed"
)

type verificationObservedStage string

const (
	verificationObservedPhysicalHash   verificationObservedStage = "physical_hash"
	verificationObservedCompressedHash verificationObservedStage = "compressed_hash"
	verificationObservedDecompression  verificationObservedStage = "decompression"
	verificationObservedLogicalHash    verificationObservedStage = "logical_hash"
	verificationObservedBlockComplete  verificationObservedStage = "block_verification_complete"
)

type verificationStageObservation struct {
	Layout  verificationLayout
	BlockID int64
	Stage   verificationObservedStage
}

type verificationStageObserver struct {
	mu      sync.Mutex
	observe func(verificationStageObservation)
}

type verificationStageObserverContextKey struct{}

func withVerificationStageObserver(ctx context.Context, observe func(verificationStageObservation)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observe == nil {
		return ctx
	}
	return context.WithValue(ctx, verificationStageObserverContextKey{}, &verificationStageObserver{observe: observe})
}

func observeSuccessfulVerificationStage(ctx context.Context, observation verificationStageObservation) {
	if ctx == nil {
		return
	}
	observer, _ := ctx.Value(verificationStageObserverContextKey{}).(*verificationStageObserver)
	if observer == nil || observer.observe == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.observe(observation)
}

func observePackedVerificationStage(ctx context.Context, blockID int64, stage verificationObservedStage) {
	observeSuccessfulVerificationStage(ctx, verificationStageObservation{
		Layout:  verificationLayoutPacked,
		BlockID: blockID,
		Stage:   stage,
	})
}

func observeLegacyVerificationStage(ctx context.Context, blockID int64, stage verificationObservedStage) {
	observeSuccessfulVerificationStage(ctx, verificationStageObservation{
		Layout:  verificationLayoutLegacy,
		BlockID: blockID,
		Stage:   stage,
	})
}

func observePayloadVerificationStage(ctx context.Context, blockID int64, payloads blockStagePayloads, stage verificationObservedStage) {
	if payloads.isPackedBlock {
		observePackedVerificationStage(ctx, blockID, stage)
		return
	}
	observeLegacyVerificationStage(ctx, blockID, stage)
}
