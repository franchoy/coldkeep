package verify

import (
	"fmt"
	"strings"
)

type VerifyFailure struct {
	Category    string
	Stage       VerifyStage
	BlockID     *int64
	ContainerID *int64
	Offset      *int64
	ExpectedHash string
	ActualHash   string
	Detail      string
	Cause       error
}

func (v *VerifyFailure) Error() string {
	if v == nil {
		return ""
	}
	parts := []string{v.Category}
	if v.Stage != "" {
		parts = append(parts, fmt.Sprintf("stage=%s", v.Stage))
	}
	if v.BlockID != nil {
		parts = append(parts, fmt.Sprintf("block_id=%d", *v.BlockID))
	}
	if v.ContainerID != nil {
		parts = append(parts, fmt.Sprintf("container_id=%d", *v.ContainerID))
	}
	if v.Offset != nil {
		parts = append(parts, fmt.Sprintf("offset=%d", *v.Offset))
	}
	if v.ExpectedHash != "" {
		parts = append(parts, fmt.Sprintf("expected_hash=%s", v.ExpectedHash))
	}
	if v.ActualHash != "" {
		parts = append(parts, fmt.Sprintf("actual_hash=%s", v.ActualHash))
	}
	if v.Detail != "" {
		parts = append(parts, v.Detail)
	}
	msg := strings.Join(parts, ": ")
	if v.Cause != nil {
		return msg + ": " + v.Cause.Error()
	}
	return msg
}

func (v *VerifyFailure) Unwrap() error {
	if v == nil {
		return nil
	}
	return v.Cause
}

type verifyFailureMeta struct {
	stage        VerifyStage
	blockID      *int64
	containerID  *int64
	offset       *int64
	expectedHash string
	actualHash   string
}

func int64ptr(v int64) *int64 {
	return &v
}

func verifyBlockFailureMeta(stage VerifyStage, blockID, containerID, offset int64) verifyFailureMeta {
	return verifyFailureMeta{
		stage:       stage,
		blockID:     int64ptr(blockID),
		containerID: int64ptr(containerID),
		offset:      int64ptr(offset),
	}
}

func verifyStageError(category string, meta verifyFailureMeta, detail string, cause error) error {
	return &VerifyFailure{
		Category:     category,
		Stage:        meta.stage,
		BlockID:      meta.blockID,
		ContainerID:  meta.containerID,
		Offset:       meta.offset,
		ExpectedHash: meta.expectedHash,
		ActualHash:   meta.actualHash,
		Detail:       detail,
		Cause:        cause,
	}
}