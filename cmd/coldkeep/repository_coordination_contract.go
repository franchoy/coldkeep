package main

import (
	"strings"

	"github.com/franchoy/coldkeep/internal/coordination"
)

// repositoryCoordinationPolicy is the non-acquiring Phase 11 CLI contract.
// Phase 12 will consume this policy from one top-level runtime wrapper.
type repositoryCoordinationPolicy struct {
	Required  bool
	Operation coordination.Operation
	Mode      coordination.Mode
}

func repositoryCoordinationPolicyFor(parsed parsedCommandLine) repositoryCoordinationPolicy {
	if parsed.hasFlag("help", "h") {
		return repositoryCoordinationPolicy{}
	}

	switch parsed.method {
	case "store":
		return exclusiveRepositoryPolicy(coordination.OperationStore)
	case "store-folder":
		return exclusiveRepositoryPolicy(coordination.OperationStoreFolder)
	case "restore":
		return exclusiveRepositoryPolicy(coordination.OperationRestore)
	case "remove":
		return exclusiveRepositoryPolicy(coordination.OperationRemove)
	case "repair":
		return exclusiveRepositoryPolicy(coordination.OperationRepair)
	case "gc":
		return exclusiveRepositoryPolicy(coordination.OperationGarbageCollect)
	case "stats":
		return exclusiveRepositoryPolicy(coordination.OperationStats)
	case "inspect":
		return exclusiveRepositoryPolicy(coordination.OperationInspect)
	case "list":
		return exclusiveRepositoryPolicy(coordination.OperationList)
	case "search":
		return exclusiveRepositoryPolicy(coordination.OperationSearch)
	case "verify":
		return exclusiveRepositoryPolicy(coordination.OperationVerify)
	case "doctor":
		return exclusiveRepositoryPolicy(coordination.OperationDoctor)
	case "config":
		return configRepositoryPolicy(parsed.positionals)
	case "snapshot":
		return snapshotRepositoryPolicy(parsed.positionals)
	default:
		// init, simulate, benchmark, version, help, and invalid commands do not
		// access the shared repository through this policy.
		return repositoryCoordinationPolicy{}
	}
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
	switch strings.ToLower(strings.TrimSpace(positionals[0])) {
	case "create":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotCreate)
	case "delete":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotDelete)
	case "restore":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotRestore)
	case "list":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotList)
	case "show":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotShow)
	case "stats":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotStats)
	case "diff":
		return exclusiveRepositoryPolicy(coordination.OperationSnapshotDiff)
	default:
		return repositoryCoordinationPolicy{}
	}
}
