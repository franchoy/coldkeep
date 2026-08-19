package main

import (
	"strings"

	"github.com/franchoy/coldkeep/internal/coordination"
)

// repositoryCoordinationPolicy is the Phase 11 command classification consumed
// by the single top-level repository lease wrapper.
type repositoryCoordinationPolicy struct {
	Required  bool
	Operation coordination.Operation
	Mode      coordination.Mode
}

var repositoryOperations = map[string]coordination.Operation{
	"store":        coordination.OperationStore,
	"store-folder": coordination.OperationStoreFolder,
	"restore":      coordination.OperationRestore,
	"remove":       coordination.OperationRemove,
	"repair":       coordination.OperationRepair,
	"gc":           coordination.OperationGarbageCollect,
	"stats":        coordination.OperationStats,
	"inspect":      coordination.OperationInspect,
	"list":         coordination.OperationList,
	"search":       coordination.OperationSearch,
	"verify":       coordination.OperationVerify,
	"doctor":       coordination.OperationDoctor,
}

var snapshotOperations = map[string]coordination.Operation{
	"create":  coordination.OperationSnapshotCreate,
	"delete":  coordination.OperationSnapshotDelete,
	"restore": coordination.OperationSnapshotRestore,
	"list":    coordination.OperationSnapshotList,
	"show":    coordination.OperationSnapshotShow,
	"stats":   coordination.OperationSnapshotStats,
	"diff":    coordination.OperationSnapshotDiff,
}

func repositoryCoordinationPolicyFor(parsed parsedCommandLine) repositoryCoordinationPolicy {
	if parsed.hasFlag("help", "h") {
		return repositoryCoordinationPolicy{}
	}

	if operation, ok := repositoryOperations[parsed.method]; ok {
		return exclusiveRepositoryPolicy(operation)
	}
	switch parsed.method {
	case "config":
		return configRepositoryPolicy(parsed.positionals)
	case "snapshot":
		return snapshotRepositoryPolicy(parsed.positionals)
	}
	// init, simulate, benchmark, version, help, and invalid commands do not
	// access the shared repository through this policy.
	return repositoryCoordinationPolicy{}
}

func exclusiveRepositoryPolicy(operation coordination.Operation) repositoryCoordinationPolicy {
	return repositoryCoordinationPolicy{
		Required:  true,
		Operation: operation,
		Mode:      coordination.ModeExclusive,
	}
}

func configRepositoryPolicy(positionals []string) repositoryCoordinationPolicy {
	if len(positionals) == 0 {
		return repositoryCoordinationPolicy{}
	}
	switch strings.ToLower(strings.TrimSpace(positionals[0])) {
	case "get":
		return exclusiveRepositoryPolicy(coordination.OperationConfigGet)
	case "set":
		return exclusiveRepositoryPolicy(coordination.OperationConfigSet)
	default:
		return repositoryCoordinationPolicy{}
	}
}

func snapshotRepositoryPolicy(positionals []string) repositoryCoordinationPolicy {
	if len(positionals) == 0 {
		return repositoryCoordinationPolicy{}
	}
	operation, ok := snapshotOperations[strings.ToLower(strings.TrimSpace(positionals[0]))]
	if !ok {
		return repositoryCoordinationPolicy{}
	}
	return exclusiveRepositoryPolicy(operation)
}
