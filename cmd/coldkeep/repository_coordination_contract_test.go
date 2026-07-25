package main

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/coordination"
)

func TestRepositoryCoordinationPolicyRequiresExclusiveRepositoryOperations(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		positionals []string
		operation   coordination.Operation
	}{
		{name: "store", method: "store", operation: coordination.OperationStore},
		{name: "store folder", method: "store-folder", operation: coordination.OperationStoreFolder},
		{name: "restore", method: "restore", operation: coordination.OperationRestore},
		{name: "remove", method: "remove", operation: coordination.OperationRemove},
		{name: "repair", method: "repair", operation: coordination.OperationRepair},
		{name: "gc", method: "gc", operation: coordination.OperationGarbageCollect},
		{name: "stats", method: "stats", operation: coordination.OperationStats},
		{name: "inspect", method: "inspect", operation: coordination.OperationInspect},
		{name: "list", method: "list", operation: coordination.OperationList},
		{name: "search", method: "search", operation: coordination.OperationSearch},
		{name: "verify", method: "verify", operation: coordination.OperationVerify},
		{name: "doctor", method: "doctor", operation: coordination.OperationDoctor},
		{name: "config get", method: "config", positionals: []string{"get", "compression"}, operation: coordination.OperationConfigGet},
		{name: "config set", method: "config", positionals: []string{"set", "compression", "zstd"}, operation: coordination.OperationConfigSet},
		{name: "snapshot create", method: "snapshot", positionals: []string{"create"}, operation: coordination.OperationSnapshotCreate},
		{name: "snapshot delete", method: "snapshot", positionals: []string{"delete"}, operation: coordination.OperationSnapshotDelete},
		{name: "snapshot restore", method: "snapshot", positionals: []string{"restore"}, operation: coordination.OperationSnapshotRestore},
		{name: "snapshot list", method: "snapshot", positionals: []string{"list"}, operation: coordination.OperationSnapshotList},
		{name: "snapshot show", method: "snapshot", positionals: []string{"show"}, operation: coordination.OperationSnapshotShow},
		{name: "snapshot stats", method: "snapshot", positionals: []string{"stats"}, operation: coordination.OperationSnapshotStats},
		{name: "snapshot diff", method: "snapshot", positionals: []string{"diff"}, operation: coordination.OperationSnapshotDiff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := repositoryCoordinationPolicyFor(parsedCommandLine{
				method:      tt.method,
				positionals: tt.positionals,
				flags:       map[string][]string{},
			})
			if !policy.Required {
				t.Fatal("repository coordination was not required")
			}
			if policy.Mode != coordination.ModeExclusive {
				t.Fatalf("mode=%q want=%q", policy.Mode, coordination.ModeExclusive)
			}
			if policy.Operation != tt.operation {
				t.Fatalf("operation=%q want=%q", policy.Operation, tt.operation)
			}
		})
	}
}

func TestRepositoryCoordinationPolicyBypassesNonRepositoryCommands(t *testing.T) {
	tests := []parsedCommandLine{
		{method: "init"},
		{method: "simulate", positionals: []string{"store"}},
		{method: "benchmark", positionals: []string{"run"}},
		{method: "version"},
		{method: "help"},
		{method: "-h"},
		{method: "--help"},
		{method: "-v"},
		{method: "--version"},
		{method: ""},
		{method: "unknown"},
		{method: "snapshot"},
		{method: "snapshot", positionals: []string{"unknown"}},
		{method: "config"},
		{method: "config", positionals: []string{"unknown"}},
	}

	for _, parsed := range tests {
		parsed.flags = map[string][]string{}
		policy := repositoryCoordinationPolicyFor(parsed)
		if policy.Required || policy.Operation != "" || policy.Mode != "" {
			t.Fatalf("method=%q positionals=%v unexpectedly coordinated: %+v", parsed.method, parsed.positionals, policy)
		}
	}
}

func TestRepositoryCoordinationPolicyUsesParsedHelpAndDoubleDashSemantics(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		required bool
		want     coordination.Operation
	}{
		{name: "help before command", args: []string{"--help", "store"}},
		{name: "command help", args: []string{"store", "--help"}},
		{name: "snapshot subcommand after double dash", args: []string{"snapshot", "--", "create"}, required: true, want: coordination.OperationSnapshotCreate},
		{name: "config subcommand after double dash", args: []string{"config", "--", "get", "compression"}, required: true, want: coordination.OperationConfigGet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseCommandLine(tt.args, flagsWithValues)
			if err != nil {
				t.Fatalf("parseCommandLine(%v): %v", tt.args, err)
			}
			policy := repositoryCoordinationPolicyFor(parsed)
			if policy.Required != tt.required || policy.Operation != tt.want {
				t.Fatalf("policy=%+v required=%v operation=%q", policy, tt.required, tt.want)
			}
			if tt.required && policy.Mode != coordination.ModeExclusive {
				t.Fatalf("mode=%q want=%q", policy.Mode, coordination.ModeExclusive)
			}
		})
	}
}

func TestRepositoryCoordinationPolicyBypassesCommandHelp(t *testing.T) {
	for _, flag := range []string{"help", "h"} {
		policy := repositoryCoordinationPolicyFor(parsedCommandLine{
			method: "store",
			flags:  map[string][]string{flag: {""}},
		})
		if policy.Required {
			t.Fatalf("--%s unexpectedly required coordination", flag)
		}
	}
}
