package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/chunk/benchmark"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/snapshot"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/internal/version"
)

const (
	exitSuccess  = 0
	exitGeneral  = 1
	exitUsage    = 2
	exitVerify   = 3
	exitRecovery = 4
)

var stdoutRedirectMu sync.Mutex

var flagsWithValues = map[string]bool{
	"codec":           true,
	"destination":     true,
	"filter":          true,
	"input":           true,
	"limit":           true,
	"mode":            true,
	"offset":          true,
	"name":            true,
	"id":              true,
	"from":            true,
	"label":           true,
	"since":           true,
	"type":            true,
	"until":           true,
	"min-size":        true,
	"max-size":        true,
	"output":          true,
	"stored-path":     true,
	"path":            true,
	"prefix":          true,
	"pattern":         true,
	"regex":           true,
	"modified-after":  true,
	"modified-before": true,
}

type cliOutputMode string

const (
	outputModeText cliOutputMode = "text"
	outputModeJSON cliOutputMode = "json"
)

type parsedCommandLine struct {
	method      string
	positionals []string
	flags       map[string][]string
}

// doctorReport is the stable v1.0 JSON data payload for `coldkeep doctor --output json`.
// All fields are frozen API: do not remove or rename fields without a major version bump.
//
// The Recovery field intentionally includes the full recovery.Report counter set
// (aborted_logical_files, aborted_chunks, quarantined_missing, quarantined_corrupt_tail,
// quarantined_orphan, skipped_dir_entries, checked_container_record, checked_disk_files,
// sealing_completed, sealing_quarantined). These counters are actionable for operators
// and monitoring scripts: any non-zero quarantined_* or aborted_* value signals that
// corrective action was taken and should trigger alerting or human review.
// Including the full report here is a deliberate decision, not an oversight.
type doctorReport struct {
	Recovery       recovery.Report `json:"recovery"`
	VerifyLevel    string          `json:"verify_level"`
	SchemaVersion  int64           `json:"schema_version"`
	RecoveryStatus string          `json:"recovery_status"`
	VerifyStatus   string          `json:"verify_status"`
	SchemaStatus   string          `json:"schema_status"`
	physicalAudit  verify.PhysicalFileIntegritySummary
	snapshotAudit  verify.SnapshotReachabilityIntegritySummary
}

// Frozen v1.0 product contract: doctor is the fast corrective recovery + health gate.
// Default remains `standard`; operators can opt into `--full` / `--deep`.
const doctorDefaultVerifyLevel = verify.VerifyStandard

const doctorOperationalHint = "After significant operations, run coldkeep doctor to validate system health."

var doctorRecoveryPhase = recovery.SystemRecoveryReportWithContainersDir
var doctorSchemaVersionPhase = querySchemaVersion
var doctorVerifyPhase = maintenance.VerifyCommandWithContainersDir
var doctorSystemAuditPhase = maintenance.CollectSystemAuditSummary
var repairLogicalRefCountsPhase = maintenance.RepairLogicalRefCountsResultRun
var runGCPhase = maintenance.RunGCWithContainersDirResult
var startupRecoveryPhase = recovery.SystemRecoveryReportWithContainersDir
var loadDefaultStorageContextPhase = storage.LoadDefaultStorageContext
var createSnapshotPhase = snapshot.CreateSnapshotWithOptions
var restoreSnapshotPhase = snapshot.RestoreSnapshot
var listSnapshotsPhase = snapshot.ListSnapshots
var getSnapshotPhase = snapshot.GetSnapshot
var listSnapshotFilesPhase = snapshot.ListSnapshotFiles
var snapshotStatsPhase = snapshot.GetSnapshotStats
var deleteSnapshotPhase = snapshot.DeleteSnapshot
var snapshotDeleteLineagePreviewPhase = loadSnapshotDeleteLineagePreview
var diffSnapshotsPhase = snapshot.DiffSnapshots
var diffSnapshotSummaryPhase = snapshot.DiffSnapshotsSummarySQL
var newObservabilityServicePhase = observability.NewService
var runObservabilityStatsPhase = func(opts observability.StatsOptions) (*observability.StatsResult, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	svc, err := newObservabilityServicePhase(sgctx.DB)
	if err != nil {
		return nil, err
	}

	r, err := svc.Stats(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	return r, nil
}

var runStatsPhase = func() (*maintenance.StatsResult, error) {
	r, err := runObservabilityStatsPhase(observability.StatsOptions{IncludeContainers: true})
	if err != nil {
		return nil, err
	}

	return mapObservabilityStatsToMaintenance(r), nil
}
var runObservabilityInspectPhase = func(entity observability.EntityType, id string, opts observability.InspectOptions) (*observability.InspectResult, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	defer func() { _ = sgctx.DB.Close() }()

	svc, err := newObservabilityServicePhase(sgctx.DB)
	if err != nil {
		return nil, err
	}

	r, err := svc.Inspect(context.Background(), entity, id, opts)
	if err != nil {
		return nil, err
	}

	return r, nil
}
var inspectLogicalFilePhase = func(dbconn *sql.DB, fileID int64) (storage.LogicalFileInspectInfo, error) {
	svc, err := newObservabilityServicePhase(dbconn)
	if err != nil {
		return storage.LogicalFileInspectInfo{}, err
	}

	r, err := svc.Inspect(context.Background(), observability.EntityFile, strconv.FormatInt(fileID, 10), observability.InspectOptions{})
	if err != nil {
		if errors.Is(err, observability.ErrNotFound) {
			return storage.LogicalFileInspectInfo{}, sql.ErrNoRows
		}
		return storage.LogicalFileInspectInfo{}, err
	}

	chunkCount, err := toInt64FromAny(r.Summary["chunk_count"])
	if err != nil {
		return storage.LogicalFileInspectInfo{}, fmt.Errorf("inspect logical file %d: parse chunk_count: %w", fileID, err)
	}
	avgChunkSizeBytes, err := toFloat64FromAny(r.Summary["avg_chunk_size_bytes"])
	if err != nil {
		return storage.LogicalFileInspectInfo{}, fmt.Errorf("inspect logical file %d: parse avg_chunk_size_bytes: %w", fileID, err)
	}
	chunkerVersion, err := toStringFromAny(r.Summary["chunker_version"])
	if err != nil {
		return storage.LogicalFileInspectInfo{}, fmt.Errorf("inspect logical file %d: parse chunker_version: %w", fileID, err)
	}

	return storage.LogicalFileInspectInfo{
		FileID:            fileID,
		ChunkerVersion:    chunk.Version(chunkerVersion),
		ChunkCount:        chunkCount,
		AvgChunkSizeBytes: avgChunkSizeBytes,
	}, nil
}
var runChunkerBenchmarkPhase = runChunkerBenchmark
var isDeprecatedChunkerVersionPhase = func(v chunk.Version) (bool, string) {
	// Future-proof policy hook: no deprecated chunkers currently.
	return false, ""
}

type cliError struct {
	code int
	msg  string
	err  error
}

func (e *cliError) Error() string {
	if e.msg != "" {
		return e.msg
	}
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func (e *cliError) Unwrap() error {
	return e.err
}

func usageErrorf(format string, args ...any) error {
	return &cliError{code: exitUsage, msg: fmt.Sprintf(format, args...)}
}

func verifyError(err error) error {
	if err == nil {
		return nil
	}
	return &cliError{code: exitVerify, err: err}
}

func recoveryError(err error) error {
	if err == nil {
		return nil
	}
	return &cliError{code: exitRecovery, err: err}
}

func main() {
	code := runCLI(os.Args[1:])
	if code != exitSuccess {
		os.Exit(code)
	}
}

func runCLI(args []string) int {
	startupMode := inferOutputModeFromArgs(args)
	if startupMode == outputModeJSON {
		prevOutput := log.Writer()
		prevFlags := log.Flags()
		log.SetOutput(io.Discard)
		log.SetFlags(0)
		defer func() {
			log.SetOutput(prevOutput)
			log.SetFlags(prevFlags)
		}()
	}

	if len(args) < 1 {
		printHelp()
		return exitSuccess
	}

	if shouldRunStartupRecovery(args[0]) {
		recoveryReport, recoveryErr := runStartupRecoveryWithOptionalLogBuffering(startupMode)
		if recoveryErr != nil {
			log.Printf("System recovery failed: %v\n", recoveryErr)
		}
		emitStartupRecoveryReport(startupMode, recoveryReport, recoveryErr)

		if startupMode != outputModeJSON {
			checkEnvFilePermissions()
		}
	}

	parsed, err := parseCommandLine(args, flagsWithValues)
	if err != nil {
		return printCLIError(err, startupMode)
	}

	outputMode, err := resolveOutputMode(parsed)
	if err != nil {
		return printCLIError(err, startupMode)
	}

	switch parsed.method {
	case "init":
		err = initCommand()
	case "config":
		err = runConfigCommand(parsed, outputMode)
	case "doctor":
		err = runDoctorCommand(parsed, outputMode)
	case "store":
		err = runStoreCommand(parsed, outputMode)
	case "store-folder":
		err = runStoreFolderCommand(parsed, outputMode)
	case "restore":
		err = runRestoreCommand(parsed, outputMode)
	case "remove":
		err = runRemoveCommand(parsed, outputMode)
	case "repair":
		err = runRepairCommand(parsed, outputMode)
	case "gc":
		err = runGCCommand(parsed, outputMode)
	case "simulate":
		err = runSimulateCommand(parsed, outputMode)
	case "benchmark":
		err = runBenchmarkCommand(parsed, outputMode)
	case "stats":
		err = runStatsCommand(parsed, outputMode)
	case "inspect":
		err = runInspectCommand(parsed, outputMode)
	case "help", "-h", "--help":
		printHelp()
	case "version", "-v", "--version":
		err = runVersionCommand(outputMode)
	case "list":
		err = runListCommand(parsed, outputMode)
	case "search":
		err = runSearchCommand(parsed, outputMode)
	case "verify":
		err = runVerifyCommand(parsed, outputMode)
	case "snapshot":
		err = runSnapshotCommand(parsed, outputMode)
	default:
		err = usageErrorf("unknown command: %s", parsed.method)
	}

	if err != nil {
		return printCLIError(err, outputMode)
	}

	printCLISuccess(parsed, outputMode)

	return exitSuccess
}

func runStartupRecoveryWithOptionalLogBuffering(mode cliOutputMode) (recovery.Report, error) {
	if mode != outputModeText || !isQuietHealthyStartupRecoveryEnabled() {
		return startupRecoveryPhase(container.ContainersDir)
	}

	prevOutput := log.Writer()
	prevFlags := log.Flags()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	recoveryReport, recoveryErr := startupRecoveryPhase(container.ContainersDir)
	if shouldReplayBufferedRecoveryLogs(recoveryReport, recoveryErr) {
		if _, err := io.Copy(prevOutput, &buf); err != nil {
			log.Printf("failed to replay buffered startup recovery logs: %v", err)
		}
	}

	return recoveryReport, recoveryErr
}

func isQuietHealthyStartupRecoveryEnabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY")))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func shouldReplayBufferedRecoveryLogs(report recovery.Report, err error) bool {
	if err != nil {
		return true
	}
	if report.AbortedLogicalFiles > 0 || report.AbortedChunks > 0 {
		return true
	}
	if report.QuarantinedMissing > 0 || report.QuarantinedCorruptTail > 0 || report.QuarantinedOrphan > 0 {
		return true
	}
	if report.SealingCompleted > 0 || report.SealingQuarantined > 0 {
		return true
	}
	return false
}

func emitStartupRecoveryReport(mode cliOutputMode, report recovery.Report, err error) {
	if mode == outputModeText && isQuietHealthyStartupRecoveryEnabled() && !shouldReplayBufferedRecoveryLogs(report, err) {
		return
	}

	if mode == outputModeJSON {
		// Startup recovery JSON is an event-style diagnostic stream on stderr.
		// It is intentionally separate from command result contracts on stdout.
		payload := map[string]any{
			"event":                               "startup_recovery",
			"status":                              "ok",
			"aborted_logical_files":               report.AbortedLogicalFiles,
			"aborted_chunks":                      report.AbortedChunks,
			"quarantined_missing_containers":      report.QuarantinedMissing,
			"quarantined_corrupt_tail_containers": report.QuarantinedCorruptTail,
			"quarantined_orphan_containers":       report.QuarantinedOrphan,
			"checked_container_records":           report.CheckedContainerRecord,
			"checked_disk_files":                  report.CheckedDiskFiles,
			"skipped_dir_entries":                 report.SkippedDirEntries,
		}
		if err != nil {
			payload["status"] = "error"
			payload["message"] = strings.TrimSpace(err.Error())
		}
		encoded, _ := json.Marshal(payload)
		fmt.Fprintln(os.Stderr, string(encoded))
		return
	}

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"RECOVERY status=error aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d checked_container_records=%d checked_disk_files=%d skipped_dir_entries=%d message=%q\n",
			report.AbortedLogicalFiles,
			report.AbortedChunks,
			report.QuarantinedMissing,
			report.QuarantinedCorruptTail,
			report.QuarantinedOrphan,
			report.CheckedContainerRecord,
			report.CheckedDiskFiles,
			report.SkippedDirEntries,
			strings.TrimSpace(err.Error()),
		)
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"RECOVERY status=ok aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d checked_container_records=%d checked_disk_files=%d skipped_dir_entries=%d\n",
		report.AbortedLogicalFiles,
		report.AbortedChunks,
		report.QuarantinedMissing,
		report.QuarantinedCorruptTail,
		report.QuarantinedOrphan,
		report.CheckedContainerRecord,
		report.CheckedDiskFiles,
		report.SkippedDirEntries,
	)
}

func printCLIError(err error, mode cliOutputMode) int {
	code := classifyExitCode(err)
	invariantCode, hasInvariantCode := invariants.Code(err)
	recommendedAction := invariants.RecommendedActionForError(err)
	dbHint := localDBSetupHint(err)
	if mode == outputModeJSON {
		payload := map[string]any{
			"status":      "error",
			"error_class": exitErrorClassLabel(code),
			"exit_code":   code,
			"message":     strings.TrimSpace(err.Error()),
		}
		if hasInvariantCode {
			payload["invariant_code"] = invariantCode
		}
		if strings.TrimSpace(recommendedAction) != "" {
			payload["recommended_action"] = recommendedAction
		}
		encoded, _ := json.Marshal(payload)
		fmt.Fprintln(os.Stderr, string(encoded))
		return code
	}

	fmt.Fprintf(os.Stderr, "ERROR[%s]: %s\n", exitErrorClassLabel(code), strings.TrimSpace(err.Error()))
	if hasInvariantCode {
		fmt.Fprintf(os.Stderr, "INVARIANT_CODE: %s\n", invariantCode)
	}
	if strings.TrimSpace(recommendedAction) != "" {
		fmt.Fprintf(os.Stderr, "Recommended action: %s\n", recommendedAction)
	}
	if strings.TrimSpace(dbHint) != "" {
		fmt.Fprintln(os.Stderr, dbHint)
	}
	return code
}

func localDBSetupHint(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.Contains(msg, "failed to connect to local db") {
		return ""
	}

	missing := make([]string, 0, 6)
	for _, key := range []string{"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE"} {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			missing = append(missing, key)
		}
	}

	b := &strings.Builder{}
	b.WriteString("DB setup hint: local mode requires PostgreSQL connection env vars.")
	if len(missing) > 0 {
		b.WriteString("\nMissing/empty: ")
		b.WriteString(strings.Join(missing, ", "))
		b.WriteString("\nExample:")
		b.WriteString("\n  export DB_HOST=127.0.0.1")
		b.WriteString("\n  export DB_PORT=5432")
		b.WriteString("\n  export DB_USER=coldkeep")
		b.WriteString("\n  export DB_PASSWORD=coldkeep")
		b.WriteString("\n  export DB_NAME=coldkeep")
		b.WriteString("\n  export DB_SSLMODE=disable")
		b.WriteString("\n  export COLDKEEP_DB_AUTO_BOOTSTRAP=true")
		return b.String()
	}

	if strings.Contains(msg, "ssl is not enabled on the server") {
		b.WriteString("\nYour database rejected SSL negotiation; for local Docker PostgreSQL use DB_SSLMODE=disable.")
		return b.String()
	}

	b.WriteString("\nCheck DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME/DB_SSLMODE values and PostgreSQL availability.")
	return b.String()
}

func printCLISuccess(parsed parsedCommandLine, mode cliOutputMode) {
	if mode != outputModeJSON {
		return
	}
	// These commands emit their own structured JSON payload.
	// Keep this list in sync with TestPrintCLISuccessJSONCommandPolicy.
	switch parsed.method {
	case "store", "store-folder", "restore", "remove", "repair", "gc", "list", "search", "stats", "inspect", "simulate", "benchmark", "doctor", "snapshot", "config", "version", "-v", "--version":
		return
	}

	payload := map[string]any{
		"status":  "ok",
		"command": parsed.method,
	}

	if len(parsed.positionals) > 0 {
		payload["target"] = parsed.positionals[0]
	}
	if parsed.method == "verify" {
		if verifyLevel, err := parseVerifyLevel(parsed); err == nil {
			payload["level"] = verifyLevelToString(verifyLevel)
		}
	}

	encoded, _ := json.Marshal(payload)
	fmt.Println(string(encoded))
}

func runVersionCommand(mode cliOutputMode) error {
	if mode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "version",
			"data": map[string]any{
				"version": version.String(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Println("coldkeep version", version.String())
	return nil
}

func validateConfigDefaultChunkerVersion(raw string) (chunk.Version, error) {
	v := chunk.Version(strings.TrimSpace(raw))
	if !chunk.IsWellFormedVersion(v) {
		return "", usageErrorf("invalid default-chunker value %q: malformed version", raw)
	}
	if _, ok := chunk.DefaultRegistry().Get(v); !ok {
		return "", usageErrorf("invalid default-chunker value %q: unknown chunker version", raw)
	}
	if deprecated, reason := isDeprecatedChunkerVersionPhase(v); deprecated {
		reason = strings.TrimSpace(reason)
		if reason == "" {
			return "", usageErrorf("invalid default-chunker value %q: deprecated chunker version", raw)
		}
		return "", usageErrorf("invalid default-chunker value %q: deprecated chunker version (%s)", raw, reason)
	}
	return v, nil
}

func runConfigCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep config <get|set> default-chunker [value]")
	}

	subcommand := strings.TrimSpace(strings.ToLower(parsed.positionals[0]))
	key := strings.TrimSpace(strings.ToLower(parsed.positionals[1]))
	if key != "default-chunker" {
		return usageErrorf("unknown config key: %s", parsed.positionals[1])
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	repo := storage.NewRepository(sgctx.DB)

	switch subcommand {
	case "get":
		if len(parsed.positionals) != 2 {
			return usageErrorf("Usage: coldkeep config get default-chunker")
		}

		v, err := repo.GetDefaultChunkerVersion()
		if err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "config get",
				"data": map[string]any{
					"key":   "default-chunker",
					"value": string(v),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, string(v))
		return nil

	case "set":
		if len(parsed.positionals) != 3 {
			return usageErrorf("Usage: coldkeep config set default-chunker <value>")
		}

		v, err := validateConfigDefaultChunkerVersion(parsed.positionals[2])
		if err != nil {
			return err
		}

		previous, err := repo.GetDefaultChunkerVersion()
		if err != nil {
			return err
		}

		if err := repo.SetDefaultChunkerVersion(v); err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "config set",
				"data": map[string]any{
					"key":   "default-chunker",
					"value": string(v),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintf(os.Stdout, "default-chunker set to %s\n", v)
		if previous != v {
			_, _ = fmt.Fprintln(os.Stdout, "Warning: This affects only new stored data.")
			_, _ = fmt.Fprintln(os.Stdout, "Existing data remains unchanged.")
		}
		return nil

	default:
		return usageErrorf("unknown config subcommand: %s", parsed.positionals[0])
	}
}

func verifyLevelToString(level verify.VerifyLevel) string {
	switch level {
	case verify.VerifyStandard:
		return "standard"
	case verify.VerifyFull:
		return "full"
	case verify.VerifyDeep:
		return "deep"
	default:
		return "unknown"
	}
}

func resolveOutputMode(parsed parsedCommandLine) (cliOutputMode, error) {
	value, hasValue := parsed.lastFlagValue("output")
	hasJSONFlag := parsed.hasFlag("json")
	normalized := strings.ToLower(strings.TrimSpace(value))

	if hasJSONFlag && hasValue && normalized != "" && normalized != "json" {
		return outputModeText, usageErrorf("cannot combine --json with --output %s", normalized)
	}
	if hasJSONFlag {
		if !hasValue {
			return outputModeJSON, nil
		}
		if normalized == "json" {
			return outputModeJSON, nil
		}
	}

	if !hasValue {
		return outputModeText, nil
	}

	switch normalized {
	case "", "text", "human":
		return outputModeText, nil
	case "json":
		return outputModeJSON, nil
	default:
		return outputModeText, usageErrorf("invalid --output value %q (allowed: human, text, json)", value)
	}
}

var outputSupportedCommands = map[string]bool{
	"config":       true,
	"doctor":       true,
	"verify":       true,
	"list":         true,
	"search":       true,
	"stats":        true,
	"store":        true,
	"store-folder": true,
	"restore":      true,
	"remove":       true,
	"repair":       true,
	"gc":           true,
	"simulate":     true,
	"benchmark":    true,
	"snapshot":     true,
}

func inferOutputModeFromArgs(args []string) cliOutputMode {
	if len(args) < 1 || !outputSupportedCommands[args[0]] {
		return outputModeText
	}
	if args[0] == "stats" {
		for i := 1; i < len(args); i++ {
			if args[i] == "--json" {
				return outputModeJSON
			}
		}
	}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--output=") {
			if strings.EqualFold(strings.TrimPrefix(arg, "--output="), "json") {
				return outputModeJSON
			}
		}
		if arg == "--output" && i+1 < len(args) {
			if strings.EqualFold(args[i+1], "json") {
				return outputModeJSON
			}
		}
	}

	return outputModeText
}

func shouldRunStartupRecovery(command string) bool {
	switch command {
	// doctor runs its own corrective recovery phase inside runDoctorCommand so it can
	// report corrective recovery/verify/schema in a single command-specific payload.
	case "store", "store-folder", "restore", "remove", "repair", "gc", "stats", "inspect", "list", "search", "verify", "snapshot":
		return true
	default:
		return false
	}
}

func exitErrorClassLabel(code int) string {
	switch code {
	case exitUsage:
		return "USAGE"
	case exitVerify:
		return "VERIFY"
	case exitRecovery:
		return "RECOVERY"
	default:
		return "GENERAL"
	}
}

// Keep fallback matching intentionally narrow; typed cliError classifications are authoritative.
// Usage-like verify parser errors are handled in the usage branch inside classifyExitCode.
func isLikelyVerifyFailureMessage(msg string) bool {
	if strings.Contains(msg, "verification failed") {
		return true
	}

	return strings.Contains(msg, "verify phase failed") ||
		strings.Contains(msg, "verify command failed")
}

func classifyExitCode(err error) int {
	if err == nil {
		return exitSuccess
	}

	var ce *cliError
	if errors.As(err, &ce) {
		switch ce.code {
		case exitUsage:
			return exitUsage
		case exitVerify:
			return exitVerify
		case exitRecovery:
			return exitRecovery
		default:
			return exitGeneral
		}
	}

	msg := strings.ToLower(strings.TrimSpace(err.Error()))

	if strings.Contains(msg, "usage:") ||
		strings.Contains(msg, "missing command") ||
		strings.Contains(msg, "missing value for --") ||
		strings.Contains(msg, "unknown flag(s)") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unknown option for gc") ||
		strings.Contains(msg, "no valid file ids after parsing input") ||
		strings.Contains(msg, "invalid fileid") ||
		strings.Contains(msg, "invalid file id") ||
		strings.Contains(msg, "unknown target for verify") ||
		strings.Contains(msg, "unknown verify level") ||
		strings.Contains(msg, "multiple verify levels provided") ||
		strings.Contains(msg, "verify level provided both as flag and positional argument") ||
		strings.Contains(msg, "invalid --limit") ||
		strings.Contains(msg, "invalid --min-size") ||
		strings.Contains(msg, "invalid --offset") ||
		strings.Contains(msg, "invalid --max-size") ||
		strings.Contains(msg, "unknown simulate subcommand") {
		return exitUsage
	}

	if isLikelyVerifyFailureMessage(msg) {
		return exitVerify
	}

	if strings.Contains(msg, "recovery phase failed") || strings.Contains(msg, "system recovery failed") {
		return exitRecovery
	}

	return exitGeneral
}

func runStoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "codec", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep store [--codec <plain|aes-gcm>] <filePath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.lastFlagValue("codec")

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	var result storage.StoreFileResult
	if codecName == "" {
		result, err = storage.StoreFileWithStorageContextResult(sgctx, path)
	} else {
		if codecName == "plain" {
			_, _ = fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}

		codec, parseErr := blocks.ParseCodec(codecName)
		if parseErr != nil {
			return parseErr
		}

		result, err = storage.StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	}
	if sgctx.Writer != nil {
		_ = sgctx.Writer.FinalizeContainer()
	}
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "store",
			"data": map[string]any{
				"path":           result.Path,
				"stored_path":    result.Path,
				"file_id":        result.FileID,
				"file_hash":      result.FileHash,
				"already_stored": result.AlreadyStored,
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if result.AlreadyStored {
		_, _ = fmt.Fprintln(os.Stdout, "File already stored: "+result.Path)
	} else {
		_, _ = fmt.Fprintln(os.Stdout, "File stored successfully: "+result.Path)
	}
	_, _ = fmt.Fprintln(os.Stdout, "  FileID: "+strconv.FormatInt(result.FileID, 10))
	_, _ = fmt.Fprintln(os.Stdout, "  SHA256: "+result.FileHash)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runStoreFolderCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "codec", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep store-folder [--codec <plain|aes-gcm>] <folderPath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.lastFlagValue("codec")

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if codecName == "" {
		err = storage.StoreFolderWithStorageContext(sgctx, path)
	} else {
		if codecName == "plain" {
			_, _ = fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}

		codec, parseErr := blocks.ParseCodec(codecName)
		if parseErr != nil {
			return parseErr
		}

		err = storage.StoreFolderWithStorageContextAndCodec(sgctx, path, codec)
	}
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "store-folder",
			"target":  path,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	_, _ = fmt.Fprintln(os.Stdout, "Folder stored successfully: "+path)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runRestoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "input", "dry-run", "dryRun", "fail-fast", "failFast", "overwrite", "stored-path", "mode", "destination", "strict", "no-metadata"); err != nil {
		return err
	}

	storedPath, _ := parsed.lastFlagValue("stored-path")
	hasStoredPath := strings.TrimSpace(storedPath) != ""
	overwrite := parsed.hasFlag("overwrite")

	if hasStoredPath {
		if len(parsed.positionals) != 0 {
			return usageErrorf("Usage: coldkeep restore --stored-path <path> [--mode <original|prefix|override>] [--destination <path>] [--overwrite] [--strict] [--no-metadata]")
		}
		if parsed.hasFlag("input") {
			return usageErrorf("--input is not supported with --stored-path")
		}
		if parsed.hasFlag("dry-run", "dryRun", "fail-fast", "failFast") {
			return usageErrorf("--dry-run and --fail-fast are not supported with --stored-path")
		}

		strictMetadata := parsed.hasFlag("strict")
		noMetadata := parsed.hasFlag("no-metadata")
		if strictMetadata && noMetadata {
			return usageErrorf("--strict and --no-metadata cannot be used together")
		}

		destinationMode, err := parseRestoreDestinationMode(parsed)
		if err != nil {
			return err
		}
		destination, _ := parsed.lastFlagValue("destination")
		destination = strings.TrimSpace(destination)
		if destinationMode == storage.RestoreDestinationOriginal && destination != "" {
			return usageErrorf("--destination is only supported with --mode prefix or --mode override")
		}
		if (destinationMode == storage.RestoreDestinationPrefix || destinationMode == storage.RestoreDestinationOverride) && destination == "" {
			return usageErrorf("--destination is required with --mode %s", destinationMode)
		}

		sgctx, err := storage.LoadDefaultStorageContext()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		result, err := storage.RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, storage.RestoreOptions{
			Overwrite:       overwrite,
			DestinationMode: destinationMode,
			Destination:     destination,
			StrictMetadata:  strictMetadata,
			NoMetadata:      noMetadata,
		})
		if err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "restore",
				"data": map[string]any{
					"stored_path":   strings.TrimSpace(storedPath),
					"output_path":   result.OutputPath,
					"file_id":       result.FileID,
					"restored_hash": result.RestoredHash,
					"mode":          destinationMode,
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, "File restored successfully: "+result.OutputPath)
		_, _ = fmt.Fprintln(os.Stdout, "  FileID: "+strconv.FormatInt(result.FileID, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  SHA256: "+result.RestoredHash)
		_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
		return nil
	}

	if parsed.hasFlag("strict", "no-metadata") {
		return usageErrorf("--strict and --no-metadata are only supported with --stored-path")
	}

	inputFile, _ := parsed.lastFlagValue("input")
	hasInput := strings.TrimSpace(inputFile) != ""
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}
	if !hasInput && len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	failFast := parsed.hasFlag("fail-fast", "failFast")
	targetArgs := parsed.positionals[:len(parsed.positionals)-1]
	outputRoot := parsed.positionals[len(parsed.positionals)-1]
	if !hasInput && len(targetArgs) == 1 {
		target := strings.TrimSpace(targetArgs[0])
		id, parseErr := strconv.ParseInt(target, 10, 64)
		if parseErr != nil || id <= 0 {
			return usageErrorf("Invalid fileID: %s (restore expects numeric logical file IDs; for path-based restore use --stored-path)\nDid you mean: coldkeep restore --stored-path <path> --destination <outputPath> --mode override", targetArgs[0])
		}
	}

	rawTargets, err := batch.LoadRawTargets(targetArgs, inputFile)
	if err != nil {
		return usageErrorf("failed to open/read input file: %v", err)
	}
	preparedTargets := batch.PrepareTargets(rawTargets)
	// Defensive fallback: empty prepared targets can still happen when no
	// materialized IDs are provided (for example, empty args/input combinations).
	if len(preparedTargets) == 0 {
		return usageErrorf("no valid file IDs after parsing input")
	}
	if !batch.HasExecutableTargets(preparedTargets) {
		report := batch.ExecutePrepared(batch.OperationRestore, dryRun, failFast, preparedTargets, nil)
		return emitBatchCommandReport("restore", report, outputMode)
	}

	outputPath, err := ensureRestoreOutputDir(outputRoot, !dryRun)
	if err != nil {
		return err
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	execFunc := func(fileID int64) batch.ItemResult {
		if dryRun {
			return executeRestoreDryRunItem(sgctx.DB, fileID, outputPath, overwrite)
		}
		return executeRestoreItem(&sgctx, fileID, outputPath, overwrite)
	}

	report := batch.ExecutePrepared(batch.OperationRestore, dryRun, failFast, preparedTargets, execFunc)
	return emitBatchCommandReport("restore", report, outputMode)
}

func parseRestoreDestinationMode(parsed parsedCommandLine) (storage.RestoreDestinationMode, error) {
	value, hasValue := parsed.lastFlagValue("mode")
	if !hasValue {
		return storage.RestoreDestinationOriginal, nil
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(storage.RestoreDestinationOriginal):
		return storage.RestoreDestinationOriginal, nil
	case string(storage.RestoreDestinationPrefix):
		return storage.RestoreDestinationPrefix, nil
	case string(storage.RestoreDestinationOverride):
		return storage.RestoreDestinationOverride, nil
	default:
		return "", usageErrorf("invalid --mode value %q (allowed: original, prefix, override)", value)
	}
}

func runRemoveCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "input", "dry-run", "dryRun", "fail-fast", "failFast", "stored-path", "stored-paths"); err != nil {
		return err
	}

	storedPath, _ := parsed.lastFlagValue("stored-path")
	hasStoredPath := strings.TrimSpace(storedPath) != ""
	storedPathsMode := parsed.hasFlag("stored-paths")
	if storedPathsMode && hasStoredPath {
		return usageErrorf("--stored-path and --stored-paths cannot be used together")
	}
	if hasStoredPath {
		if len(parsed.positionals) != 0 {
			return usageErrorf("Usage: coldkeep remove --stored-path <path>")
		}
		if parsed.hasFlag("input") {
			return usageErrorf("--input is not supported with --stored-path")
		}
		if parsed.hasFlag("dry-run", "dryRun", "fail-fast", "failFast") {
			return usageErrorf("--dry-run and --fail-fast are not supported with --stored-path")
		}

		sgctx, err := storage.LoadDefaultStorageContext()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		result, err := storage.RemoveFileByStoredPathWithStorageContextResult(sgctx, storedPath)
		if err != nil {
			return err
		}

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "remove",
				"data": map[string]any{
					"stored_path":         result.StoredPath,
					"logical_file_id":     result.LogicalFileID,
					"remaining_ref_count": result.RemainingRefCount,
					"removed":             result.Removed,
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintln(os.Stdout, "Stored path mapping removed: "+result.StoredPath)
		_, _ = fmt.Fprintln(os.Stdout, "  LogicalFileID: "+strconv.FormatInt(result.LogicalFileID, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  Remaining refs: "+strconv.FormatInt(result.RemainingRefCount, 10))
		_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
		return nil
	}

	if storedPathsMode {
		inputFile, _ := parsed.lastFlagValue("input")
		hasInput := strings.TrimSpace(inputFile) != ""
		if !hasInput && len(parsed.positionals) < 1 {
			return usageErrorf("Usage: coldkeep remove --stored-paths <path> [path ...]")
		}
		dryRun := parsed.hasFlag("dry-run", "dryRun")
		failFast := parsed.hasFlag("fail-fast", "failFast")

		rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
		if err != nil {
			return usageErrorf("failed to open/read input file: %v", err)
		}
		preparedTargets := prepareRemoveStoredPathTargets(rawTargets)
		if len(preparedTargets) == 0 {
			return usageErrorf("no valid stored paths after parsing input")
		}
		if !hasExecutableRemoveStoredPathTarget(preparedTargets) {
			report := executeRemoveStoredPathPrepared(dryRun, failFast, preparedTargets, nil)
			return emitBatchCommandReport("remove", report, outputMode)
		}

		sgctx, err := storage.LoadDefaultStorageContext()
		if err != nil {
			return fmt.Errorf("load storage context: %w", err)
		}
		defer func() { _ = sgctx.Close() }()

		report := executeRemoveStoredPathPrepared(dryRun, failFast, preparedTargets, &sgctx)
		return emitBatchCommandReport("remove", report, outputMode)
	}

	inputFile, _ := parsed.lastFlagValue("input")
	hasInput := strings.TrimSpace(inputFile) != ""
	if !hasInput && len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep remove <fileID> [fileID ...]")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	failFast := parsed.hasFlag("fail-fast", "failFast")

	rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
	if err != nil {
		return usageErrorf("failed to open/read input file: %v", err)
	}
	preparedTargets := batch.PrepareTargets(rawTargets)
	// Defensive fallback: empty prepared targets can still happen when no
	// materialized IDs are provided (for example, empty args/input combinations).
	if len(preparedTargets) == 0 {
		return usageErrorf("no valid file IDs after parsing input")
	}
	if !batch.HasExecutableTargets(preparedTargets) {
		report := batch.ExecutePrepared(batch.OperationRemove, dryRun, failFast, preparedTargets, nil)
		return emitBatchCommandReport("remove", report, outputMode)
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	execFunc := func(fileID int64) batch.ItemResult {
		if dryRun {
			return executeRemoveDryRunItem(sgctx.DB, fileID)
		}
		return executeRemoveItem(&sgctx, fileID)
	}

	report := batch.ExecutePrepared(batch.OperationRemove, dryRun, failFast, preparedTargets, execFunc)
	return emitBatchCommandReport("remove", report, outputMode)
}

func ensureRestoreOutputDir(path string, createIfMissing bool) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", usageErrorf("Usage: coldkeep restore <fileID> [fileID ...] <outputDir>")
	}

	if createIfMissing {
		if err := os.MkdirAll(path, 0755); err != nil {
			return "", fmt.Errorf("create output directory %q: %w", path, err)
		}
	}

	st, err := os.Stat(path)
	if err != nil {
		if !createIfMissing && os.IsNotExist(err) {
			return path, nil
		}
		return "", fmt.Errorf("stat output directory %q: %w", path, err)
	}
	if !st.IsDir() {
		return "", usageErrorf("restore output destination must be a directory: %s", path)
	}

	return path, nil
}

func batchOverallStatus(report batch.Report) string {
	if report.Summary.Failed == 0 {
		return "ok"
	}
	if report.Summary.Success > 0 || report.Summary.Planned > 0 {
		return "partial_failure"
	}
	return "error"
}

func printBatchHumanReport(label string, report batch.Report) {
	if report.DryRun {
		fmt.Printf("[%s DRY-RUN]\n", label)
	} else {
		fmt.Printf("[%s]\n", label)
	}
	for _, item := range report.Results {
		switch item.Status {
		case batch.ResultSuccess:
			if item.OutputPath != "" {
				fmt.Printf("✔ id=%-6d -> %s\n", item.ID, item.OutputPath)
			} else if item.ID > 0 && item.Message != "" {
				fmt.Printf("✔ id=%-6d %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" && item.Message != "" {
				fmt.Printf("✔ input=%q %s\n", item.RawValue, item.Message)
			} else if item.Message != "" {
				fmt.Printf("✔ %s\n", item.Message)
			} else {
				fmt.Printf("✔ success\n")
			}
		case batch.ResultFailed:
			if item.ID > 0 {
				fmt.Printf("✖ id=%-6d error=%s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("✖ input=%q error=%s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("✖ error=%s\n", item.Message)
			}
			if strings.TrimSpace(item.InvariantCode) != "" {
				fmt.Printf("  invariant_code=%s\n", item.InvariantCode)
			}
			if strings.TrimSpace(item.RecommendedAction) != "" {
				fmt.Printf("  recommended_action=%s\n", item.RecommendedAction)
			}
		case batch.ResultSkipped:
			if item.ID > 0 {
				fmt.Printf("↷ id=%-6d skipped %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("↷ input=%q skipped %s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("↷ skipped %s\n", item.Message)
			}
		case batch.ResultPlanned:
			if item.ID > 0 {
				fmt.Printf("  id=%-6d %s\n", item.ID, item.Message)
			} else if strings.TrimSpace(item.RawValue) != "" {
				fmt.Printf("  input=%q %s\n", item.RawValue, item.Message)
			} else {
				fmt.Printf("  %s\n", item.Message)
			}
		}
	}
	fmt.Println("Summary:")
	fmt.Printf("  total:   %d\n", report.Summary.Total)
	if report.DryRun {
		fmt.Printf("  planned: %d\n", report.Summary.Planned)
		fmt.Printf("  failed:  %d\n", report.Summary.Failed)
		fmt.Printf("  skipped: %d\n", report.Summary.Skipped)
	} else {
		fmt.Printf("  success: %d\n", report.Summary.Success)
		fmt.Printf("  failed:  %d\n", report.Summary.Failed)
		fmt.Printf("  skipped: %d\n", report.Summary.Skipped)
	}
}

func executeRestoreDryRunItem(dbconn *sql.DB, fileID int64, outputDir string, overwrite bool) batch.ItemResult {
	info, err := storage.GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("file ID %d not found", fileID)}
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}

	if info.Status != filestate.LogicalFileCompleted {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("file ID %d is not COMPLETED", fileID)}
	}

	out := filepath.Join(outputDir, info.OriginalName)
	if !overwrite {
		if _, statErr := os.Stat(out); statErr == nil {
			return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("output file already exists: %s (use --overwrite)", out), OutputPath: out, OriginalName: info.OriginalName}
		} else if !os.IsNotExist(statErr) {
			return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("check output path %s: %v", out, statErr), OutputPath: out, OriginalName: info.OriginalName}
		}
	}

	return batch.ItemResult{
		ID:           fileID,
		Status:       batch.ResultPlanned,
		Message:      fmt.Sprintf("would restore -> %s", out),
		OriginalName: info.OriginalName,
		OutputPath:   out,
	}
}

func executeRestoreItem(sgctx *storage.StorageContext, fileID int64, outputDir string, overwrite bool) batch.ItemResult {
	result, err := storage.RestoreFileWithStorageContextResultOptions(*sgctx, fileID, outputDir, storage.RestoreOptions{Overwrite: overwrite})
	if err != nil {
		item := batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
		annotateBatchFailureFromError(err, &item)
		return item
	}

	return batch.ItemResult{
		ID:           fileID,
		Status:       batch.ResultSuccess,
		Message:      "restored",
		OriginalName: result.OriginalName,
		OutputPath:   result.OutputPath,
	}
}

func executeRemoveDryRunItem(dbconn *sql.DB, fileID int64) batch.ItemResult {
	info, err := storage.GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("file ID %d not found", fileID)}
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}
	if info.Status == filestate.LogicalFileProcessing {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: fmt.Sprintf("file ID %d is still PROCESSING and cannot be removed", fileID)}
	}
	return batch.ItemResult{ID: fileID, Status: batch.ResultPlanned, Message: "would remove"}
}

func executeRemoveItem(sgctx *storage.StorageContext, fileID int64) batch.ItemResult {
	result, err := storage.RemoveFileWithDBResult(sgctx.DB, fileID)
	if err != nil {
		item := batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
		annotateBatchFailureFromError(err, &item)
		return item
	}
	return batch.ItemResult{ID: fileID, Status: batch.ResultSuccess, Message: fmt.Sprintf("removed mappings=%d", result.RemovedMappings)}
}

func annotateBatchFailureFromError(err error, item *batch.ItemResult) {
	if err == nil || item == nil {
		return
	}

	code, ok := invariants.Code(err)
	if !ok {
		return
	}

	item.InvariantCode = code
	item.RecommendedAction = invariants.RecommendedActionForCode(code)
}

type preparedRemoveStoredPathTarget struct {
	StoredPath string
	Executable bool
	Result     batch.ItemResult
}

func prepareRemoveStoredPathTargets(raw []batch.RawTarget) []preparedRemoveStoredPathTarget {
	prepared := make([]preparedRemoveStoredPathTarget, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))

	for _, item := range raw {
		path := strings.TrimSpace(item.Value)
		if path == "" {
			prepared = append(prepared, preparedRemoveStoredPathTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: item.Value,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("invalid stored path %q", item.Value),
				},
			})
			continue
		}

		if _, exists := seen[path]; exists {
			prepared = append(prepared, preparedRemoveStoredPathTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: path,
					Status:   batch.ResultSkipped,
					Message:  "duplicate target",
				},
			})
			continue
		}

		seen[path] = struct{}{}
		prepared = append(prepared, preparedRemoveStoredPathTarget{StoredPath: path, Executable: true})
	}

	return prepared
}

func hasExecutableRemoveStoredPathTarget(targets []preparedRemoveStoredPathTarget) bool {
	for _, target := range targets {
		if target.Executable {
			return true
		}
	}
	return false
}

func executeRemoveStoredPathPrepared(dryRun bool, failFast bool, targets []preparedRemoveStoredPathTarget, sgctx *storage.StorageContext) batch.Report {
	results := make([]batch.ItemResult, 0, len(targets))

	for _, target := range targets {
		if !target.Executable {
			results = append(results, target.Result)
			continue
		}

		var item batch.ItemResult
		if dryRun {
			if sgctx == nil || sgctx.DB == nil {
				item = batch.ItemResult{RawValue: target.StoredPath, Status: batch.ResultFailed, Message: "internal error: storage context unavailable"}
			} else {
				item = executeRemoveStoredPathDryRunItem(sgctx.DB, target.StoredPath)
			}
		} else {
			if sgctx == nil {
				item = batch.ItemResult{RawValue: target.StoredPath, Status: batch.ResultFailed, Message: "internal error: storage context unavailable"}
			} else {
				item = executeRemoveStoredPathItem(sgctx, target.StoredPath)
			}
		}

		results = append(results, item)
		if failFast && item.Status == batch.ResultFailed {
			break
		}
	}

	report := batch.NewReport(batch.OperationRemove, dryRun, results)
	report.ExecutionMode = batch.ExecutionModeContinueOnError
	if failFast {
		report.ExecutionMode = batch.ExecutionModeFailFast
	}
	return report
}

func executeRemoveStoredPathDryRunItem(dbconn *sql.DB, storedPath string) batch.ItemResult {
	normalized := strings.TrimSpace(storedPath)
	if normalized == "" {
		return batch.ItemResult{RawValue: storedPath, Status: batch.ResultFailed, Message: fmt.Sprintf("invalid stored path %q", storedPath)}
	}

	var logicalID int64
	err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, normalized).Scan(&logicalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return batch.ItemResult{RawValue: normalized, Status: batch.ResultFailed, Message: fmt.Sprintf("physical_file[%q]: not found (never stored)", normalized)}
		}
		return batch.ItemResult{RawValue: normalized, Status: batch.ResultFailed, Message: err.Error()}
	}

	return batch.ItemResult{ID: logicalID, RawValue: normalized, Status: batch.ResultPlanned, Message: "would remove stored-path mapping"}
}

func executeRemoveStoredPathItem(sgctx *storage.StorageContext, storedPath string) batch.ItemResult {
	result, err := storage.RemoveFileByStoredPathWithStorageContextResult(*sgctx, storedPath)
	if err != nil {
		item := batch.ItemResult{RawValue: strings.TrimSpace(storedPath), Status: batch.ResultFailed, Message: err.Error()}
		annotateBatchFailureFromError(err, &item)
		return item
	}

	return batch.ItemResult{
		ID:       result.LogicalFileID,
		RawValue: result.StoredPath,
		Status:   batch.ResultSuccess,
		Message:  fmt.Sprintf("removed stored_path remaining_ref_count=%d", result.RemainingRefCount),
	}
}

func emitBatchCommandReport(command string, report batch.Report, outputMode cliOutputMode) error {
	executionMode := report.ExecutionMode
	if executionMode == "" {
		executionMode = batch.ExecutionModeContinueOnError
	}

	if outputMode == outputModeJSON {
		jsonResults := make([]map[string]any, 0, len(report.Results))
		for _, item := range report.Results {
			encoded := map[string]any{
				"status": item.Status,
			}
			if item.ID > 0 {
				encoded["id"] = item.ID
			}
			if strings.TrimSpace(item.RawValue) != "" {
				encoded["raw_value"] = item.RawValue
			}
			if item.OutputPath != "" {
				encoded["output_path"] = item.OutputPath
			}
			if item.OriginalName != "" {
				encoded["original_name"] = item.OriginalName
			}
			if strings.TrimSpace(item.InvariantCode) != "" {
				encoded["invariant_code"] = item.InvariantCode
			}
			if strings.TrimSpace(item.RecommendedAction) != "" {
				encoded["recommended_action"] = item.RecommendedAction
			}
			if item.Status == batch.ResultFailed && item.Message != "" {
				encoded["error"] = item.Message
			} else if item.Status == batch.ResultSkipped {
				message := strings.TrimSpace(item.Message)
				if message == "" {
					message = "skipped"
				}
				encoded["message"] = message
			} else if item.Message != "" {
				encoded["message"] = item.Message
			}
			jsonResults = append(jsonResults, encoded)
		}

		payload := map[string]any{
			"status":         batchOverallStatus(report),
			"command":        command,
			"dry_run":        report.DryRun,
			"execution_mode": executionMode,
			"summary":        report.Summary,
			"results":        jsonResults,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
	} else {
		printBatchHumanReport(strings.ToUpper(command), report)
		if !report.DryRun {
			fmt.Printf("  Hint: %s\n", doctorOperationalHint)
		}
	}

	if batch.ExitCodeFromReport(report) != 0 {
		return &cliError{code: deriveBatchFailureExitCode(report), msg: fmt.Sprintf("one or more %s operations failed", command)}
	}
	return nil
}

func deriveBatchFailureExitCode(report batch.Report) int {
	hasValidationFailures := false
	hasExecutionFailures := false
	hasVerifyFailures := false

	for _, item := range report.Results {
		if item.Status != batch.ResultFailed {
			continue
		}

		if strings.TrimSpace(item.InvariantCode) != "" {
			hasVerifyFailures = true
			continue
		}

		if strings.TrimSpace(item.RawValue) != "" || item.ID <= 0 {
			hasValidationFailures = true
			continue
		}

		hasExecutionFailures = true
	}

	if hasVerifyFailures {
		return exitVerify
	}
	if hasExecutionFailures {
		return exitGeneral
	}
	if hasValidationFailures {
		return exitUsage
	}
	return exitGeneral
}

func runGCCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "dry-run", "dryRun", "output"); err != nil {
		return err
	}

	dryRun := parsed.hasFlag("dry-run", "dryRun")
	switch len(parsed.positionals) {
	case 0:
	case 1:
		switch parsed.positionals[0] {
		case "dry-run", "dryRun":
			dryRun = true
		default:
			return usageErrorf("Unknown option for gc: %s", parsed.positionals[0])
		}
	default:
		return usageErrorf("Usage: coldkeep gc [--dry-run]")
	}

	result, err := runGCPhase(dryRun, container.ContainersDir)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "gc",
			"data":    result,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if result.AffectedContainers == 0 {
		fmt.Println("GC completed. No containers eligible for deletion.")
		if dryRun && result.SnapshotRetainedContainers > 0 {
			fmt.Printf("GC skipped containers still retained by snapshots: %d\n", result.SnapshotRetainedContainers)
		}
		if result.SnapshotRetainedLogicalFiles > 0 {
			fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
		}
		if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
			fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
		}
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil
	}

	if dryRun {
		for _, filename := range result.ContainerFilenames {
			fmt.Printf("[DRY-RUN] Would delete container: %s\n", filename)
		}
		fmt.Printf("GC dry-run completed. Containers eligible for deletion: %d\n", result.AffectedContainers)
		if result.SnapshotRetainedContainers > 0 {
			fmt.Printf("GC skipped containers still retained by snapshots: %d\n", result.SnapshotRetainedContainers)
		}
		if result.SnapshotRetainedLogicalFiles > 0 {
			fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
		}
		if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
			fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
		}
		fmt.Printf("Hint: %s\n", doctorOperationalHint)
		return nil
	}

	for _, filename := range result.ContainerFilenames {
		fmt.Printf("Deleted container: %s\n", filename)
	}
	fmt.Printf("GC completed. Containers deleted: %d\n", result.AffectedContainers)
	if result.SnapshotRetainedLogicalFiles > 0 {
		fmt.Printf("GC retained snapshot-protected logical files: %d\n", result.SnapshotRetainedLogicalFiles)
	}
	if result.RetainedCurrentOnlyLogical > 0 || result.RetainedSnapshotOnlyLogical > 0 || result.RetainedSharedLogical > 0 {
		fmt.Printf("GC retention roots (logical files): current_only=%d snapshot_only=%d shared=%d\n", result.RetainedCurrentOnlyLogical, result.RetainedSnapshotOnlyLogical, result.RetainedSharedLogical)
	}
	fmt.Printf("Hint: %s\n", doctorOperationalHint)
	return nil
}

func runStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "json", "containers"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep stats [--output <human|text|json>] [--json] [--containers]")
	}

	includeContainers := parsed.hasFlag("containers")
	r, err := runObservabilityStatsPhase(observability.StatsOptions{IncludeContainers: includeContainers})
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		return observability.RenderStatsJSON(os.Stdout, r)
	}

	return observability.RenderStatsHuman(os.Stdout, r)
}

func runInspectCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "relations", "reverse", "deep", "limit"); err != nil {
		return err
	}

	validEntities := map[string]observability.EntityType{
		"file":      observability.EntityFile,
		"snapshot":  observability.EntitySnapshot,
		"chunk":     observability.EntityChunk,
		"container": observability.EntityContainer,
	}

	if len(parsed.positionals) != 2 {
		return usageErrorf("Usage: coldkeep inspect (file|snapshot|chunk|container) <id>")
	}
	entityName := parsed.positionals[0]
	entityType, ok := validEntities[entityName]
	if !ok {
		return usageErrorf("Usage: coldkeep inspect (file|snapshot|chunk|container) <id>")
	}

	entityID := strings.TrimSpace(parsed.positionals[1])
	if entityID == "" {
		return usageErrorf("Invalid %s id: %q", entityName, parsed.positionals[1])
	}

	// For file/chunk/container a numeric id is required; validate early for a clear error.
	if entityType == observability.EntityFile || entityType == observability.EntityChunk || entityType == observability.EntityContainer {
		if n, err := strconv.ParseInt(entityID, 10, 64); err != nil || n <= 0 {
			return usageErrorf("Invalid %s id: %s", entityName, entityID)
		}
	}

	opts := observability.InspectOptions{
		Relations: parsed.hasFlag("relations"),
		Reverse:   parsed.hasFlag("reverse"),
		Deep:      parsed.hasFlag("deep"),
	}
	if limitStr, hasLimit := parsed.lastFlagValue("limit"); hasLimit {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n <= 0 {
			return usageErrorf("Invalid --limit value: %s", limitStr)
		}
		opts.Limit = n
	}

	r, err := runObservabilityInspectPhase(entityType, entityID, opts)
	if err != nil {
		if errors.Is(err, observability.ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s %s not found", entityName, entityID)
		}
		return err
	}

	if outputMode == outputModeJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(r)
	}

	return observability.RenderInspectHuman(os.Stdout, r)
}

func runRepairCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output", "batch", "input", "fail-fast", "failFast"); err != nil {
		return err
	}

	batchMode := parsed.hasFlag("batch")
	if batchMode {
		inputFile, _ := parsed.lastFlagValue("input")
		rawTargets, err := batch.LoadRawTargets(parsed.positionals, inputFile)
		if err != nil {
			return usageErrorf("failed to open/read input file: %v", err)
		}
		if len(rawTargets) == 0 {
			return usageErrorf("Usage: coldkeep repair ref-counts --batch [--input <file>] [--fail-fast] [--output <text|json>]")
		}

		prepared := prepareRepairTargets(rawTargets)
		if len(prepared) == 0 {
			return usageErrorf("no valid repair targets after parsing input")
		}

		report := executeRepairPrepared(parsed.hasFlag("fail-fast", "failFast"), prepared)
		return emitBatchCommandReport("repair", report, outputMode)
	}

	if len(parsed.positionals) != 1 || parsed.positionals[0] != "ref-counts" {
		return usageErrorf("Usage: coldkeep repair ref-counts [--output <text|json>]")
	}

	result, err := repairLogicalRefCountsPhase()
	if err != nil {
		return verifyError(fmt.Errorf("repair ref-counts failed: %w", err))
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "repair",
			"data": map[string]any{
				"target":                    "ref-counts",
				"scanned_logical_files":     result.ScannedLogicalFiles,
				"updated_logical_files":     result.UpdatedLogicalFiles,
				"orphan_physical_file_rows": result.OrphanPhysicalFileRows,
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Printf("Recomputed logical_file.ref_count from physical_file rows. scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d\n",
		result.ScannedLogicalFiles,
		result.UpdatedLogicalFiles,
		result.OrphanPhysicalFileRows,
	)
	fmt.Printf("Hint: %s\n", doctorOperationalHint)
	return nil
}

type preparedRepairTarget struct {
	Target     string
	Executable bool
	Result     batch.ItemResult
}

func prepareRepairTargets(raw []batch.RawTarget) []preparedRepairTarget {
	prepared := make([]preparedRepairTarget, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))

	for _, item := range raw {
		target := strings.TrimSpace(item.Value)
		if target == "" {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: item.Value,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("invalid repair target %q", item.Value),
				},
			})
			continue
		}

		if target != "ref-counts" {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: item.Value,
					Status:   batch.ResultFailed,
					Message:  fmt.Sprintf("unknown repair target %q", item.Value),
				},
			})
			continue
		}

		if _, exists := seen[target]; exists {
			prepared = append(prepared, preparedRepairTarget{
				Executable: false,
				Result: batch.ItemResult{
					RawValue: target,
					Status:   batch.ResultSkipped,
					Message:  "duplicate target",
				},
			})
			continue
		}

		seen[target] = struct{}{}
		prepared = append(prepared, preparedRepairTarget{Target: target, Executable: true})
	}

	return prepared
}

func executeRepairPrepared(failFast bool, targets []preparedRepairTarget) batch.Report {
	results := make([]batch.ItemResult, 0, len(targets))

	for _, target := range targets {
		if !target.Executable {
			results = append(results, target.Result)
			continue
		}

		result, err := repairLogicalRefCountsPhase()
		if err != nil {
			item := batch.ItemResult{
				RawValue: target.Target,
				Status:   batch.ResultFailed,
				Message:  fmt.Sprintf("repair ref-counts failed: %v", err),
			}
			if code, ok := invariants.Code(err); ok {
				item.InvariantCode = code
				item.RecommendedAction = invariants.RecommendedActionForCode(code)
			}
			results = append(results, item)
			if failFast {
				break
			}
			continue
		}

		results = append(results, batch.ItemResult{
			RawValue: target.Target,
			Status:   batch.ResultSuccess,
			Message: fmt.Sprintf(
				"repaired scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d",
				result.ScannedLogicalFiles,
				result.UpdatedLogicalFiles,
				result.OrphanPhysicalFileRows,
			),
		})
	}

	report := batch.NewReport(batch.OperationRepair, false, results)
	report.ExecutionMode = batch.ExecutionModeContinueOnError
	if failFast {
		report.ExecutionMode = batch.ExecutionModeFailFast
	}
	return report
}

func runListCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "limit", "offset", "output"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "offset"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep list [--limit <count>] [--offset <count>]")
	}
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if outputMode == outputModeJSON {
		files, err := listing.ListFilesResultWithDB(dbconn, listArgs(parsed))
		if err != nil {
			return err
		}
		if files == nil {
			files = []listing.FileRecord{}
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "list",
			"files":   files,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	files, err := listing.ListFilesResultWithDB(dbconn, listArgs(parsed))
	if err != nil {
		return err
	}
	printFileRecordsTable(files)
	return nil
}

func runSearchCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "name", "min-size", "max-size", "limit", "offset", "output"); err != nil {
		return err
	}

	// Validate numeric filter values at CLI level before forwarding to SQL.
	if err := validateNonNegativeIntegerFlag(parsed, "min-size"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "max-size"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	if err := validateNonNegativeIntegerFlag(parsed, "offset"); err != nil {
		return err
	}
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if outputMode == outputModeJSON {
		files, err := listing.SearchFilesResultWithDB(dbconn, searchArgs(parsed))
		if err != nil {
			return err
		}
		if files == nil {
			files = []listing.FileRecord{}
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "search",
			"files":   files,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	files, err := listing.SearchFilesResultWithDB(dbconn, searchArgs(parsed))
	if err != nil {
		return err
	}
	printFileRecordsTable(files)
	return nil
}

func printFileRecordsTable(records []listing.FileRecord) {
	fmt.Printf("%-6s %-25s %-15s %-20s\n", "ID", "PATH", "SIZE(bytes)", "CREATED_AT")
	fmt.Println("---------------------------------------------------------------------")
	for _, r := range records {
		fmt.Printf("%-6d %-25s %-15d %-20s\n", r.ID, r.Name, r.SizeBytes, r.CreatedAt)
	}
}

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func bytesToGB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024 * 1024)
}

func hasMixedRepositoryChunkerVersions(r *maintenance.StatsResult) bool {
	if r == nil {
		return false
	}

	versions := make(map[string]struct{})
	collect := func(input map[string]int64) {
		for version := range input {
			trimmed := strings.TrimSpace(version)
			if trimmed == "" || trimmed == "unknown" {
				continue
			}
			versions[trimmed] = struct{}{}
		}
	}

	collect(r.ChunkCountsByVersion)
	collect(r.LogicalFileCountsByVersion)

	return len(versions) > 1
}

func mapObservabilityStatsToMaintenance(r *observability.StatsResult) *maintenance.StatsResult {
	if r == nil {
		return &maintenance.StatsResult{}
	}

	out := &maintenance.StatsResult{
		TotalFiles:               r.Logical.TotalFiles,
		CompletedFiles:           r.Logical.CompletedFiles,
		ProcessingFiles:          r.Logical.ProcessingFiles,
		AbortedFiles:             r.Logical.AbortedFiles,
		TotalLogicalSizeBytes:    r.Logical.TotalSizeBytes,
		CompletedSizeBytes:       r.Logical.CompletedSizeBytes,
		ActiveWriteChunker:       r.Repository.ActiveWriteChunker,
		EstimatedDedupRatioPct:   r.Logical.EstimatedDedupRatioPct,
		TotalChunks:              r.Chunks.TotalChunks,
		CompletedChunks:          r.Chunks.CompletedChunks,
		CompletedChunkBytes:      r.Chunks.CompletedBytes,
		ChunkCountsByVersion:     cloneInt64MapAny(r.Chunks.CountsByVersion),
		ChunkBytesByVersion:      cloneInt64MapAny(r.Chunks.BytesByVersion),
		TotalChunkReferences:     r.Chunks.TotalReferences,
		UniqueReferencedChunks:   r.Chunks.UniqueReferenced,
		TotalContainers:          r.Containers.TotalContainers,
		HealthyContainers:        r.Containers.HealthyContainers,
		QuarantineContainers:     r.Containers.QuarantineContainers,
		TotalContainerBytes:      r.Containers.TotalBytes,
		HealthyContainerBytes:    r.Containers.HealthyBytes,
		QuarantineContainerBytes: r.Containers.QuarantineBytes,
		LiveBlockBytes:           r.Containers.LiveBlockBytes,
		DeadBlockBytes:           r.Containers.DeadBlockBytes,
		FragmentationRatioPct:    r.Containers.FragmentationRatioPct,
		SnapshotRetention: maintenance.SnapshotRetentionStats{
			CurrentOnlyLogicalFiles:        r.Retention.CurrentOnlyLogicalFiles,
			CurrentOnlyBytes:               r.Retention.CurrentOnlyBytes,
			SnapshotReferencedLogicalFiles: r.Retention.SnapshotReferencedLogicalFiles,
			SnapshotReferencedBytes:        r.Retention.SnapshotReferencedBytes,
			SnapshotOnlyLogicalFiles:       r.Retention.SnapshotOnlyLogicalFiles,
			SnapshotOnlyBytes:              r.Retention.SnapshotOnlyBytes,
			SharedLogicalFiles:             r.Retention.SharedLogicalFiles,
			SharedBytes:                    r.Retention.SharedBytes,
		},
	}

	out.Containers = make([]maintenance.ContainerStatRecord, 0, len(r.Containers.Records))
	for _, record := range r.Containers.Records {
		out.Containers = append(out.Containers, maintenance.ContainerStatRecord{
			ID:           record.ID,
			Filename:     record.Filename,
			TotalBytes:   record.TotalBytes,
			LiveBytes:    record.LiveBytes,
			DeadBytes:    record.DeadBytes,
			Quarantine:   record.Quarantine,
			LiveRatioPct: record.LiveRatioPct,
		})
	}

	if out.CompletedSizeBytes > 0 {
		out.GlobalDedupRatioPct = (1.0 - float64(out.LiveBlockBytes)/float64(out.CompletedSizeBytes)) * 100
	}

	return out
}

func cloneInt64MapAny(in map[string]int64) map[string]int64 {
	if in == nil {
		return map[string]int64{}
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func toInt64FromAny(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if math.Trunc(n) != n {
			return 0, fmt.Errorf("non-integer float value %v", n)
		}
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func toFloat64FromAny(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func toStringFromAny(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unexpected type %T", v)
	}
	return s, nil
}

func printStatsReport(r *maintenance.StatsResult) {
	fmt.Println("\n====== coldkeep Stats ======")
	if strings.TrimSpace(r.ActiveWriteChunker) != "" {
		fmt.Printf("Active chunker (new writes):     %s\n", r.ActiveWriteChunker)
	}
	fmt.Printf("Logical files (total):           %d\n", r.TotalFiles)
	fmt.Printf("Logical stored size (total):     %.2f MB\n", bytesToMB(r.TotalLogicalSizeBytes))
	fmt.Printf("  Completed files:               %d (%.2f MB)\n", r.CompletedFiles, bytesToMB(r.CompletedSizeBytes))
	fmt.Printf("  Processing files:              %d (%.2f MB)\n", r.ProcessingFiles, bytesToMB(r.ProcessingSizeBytes))
	fmt.Printf("  Aborted files:                 %d (%.2f MB)\n", r.AbortedFiles, bytesToMB(r.AbortedSizeBytes))
	fmt.Printf("Healthy containers:              %d\n", r.HealthyContainers)
	fmt.Printf("Healthy container bytes:         %.2f MB\n", bytesToMB(r.HealthyContainerBytes))
	fmt.Printf("Quarantined containers:          %d\n", r.QuarantineContainers)
	fmt.Printf("Quarantined container bytes:     %.2f MB\n", bytesToMB(r.QuarantineContainerBytes))
	fmt.Printf("Total containers:                %d\n", r.TotalContainers)
	fmt.Printf("Total container bytes:           %.2f MB\n", bytesToMB(r.TotalContainerBytes))
	fmt.Printf("Live block bytes (physical):     %.2f MB\n", bytesToMB(r.LiveBlockBytes))
	fmt.Printf("Dead block bytes (physical):     %.2f MB\n", bytesToMB(r.DeadBlockBytes))
	if r.GlobalDedupRatioPct > 0 {
		fmt.Printf("Global dedup ratio:              %.2f%%\n", r.GlobalDedupRatioPct)
	}
	if r.FragmentationRatioPct > 0 {
		fmt.Printf("Fragmentation ratio:             %.2f%%\n", r.FragmentationRatioPct)
	}
	fmt.Println("Snapshot retention:")
	fmt.Printf("  Current-only logical files:    %d (%.2f MB)\n", r.SnapshotRetention.CurrentOnlyLogicalFiles, bytesToMB(r.SnapshotRetention.CurrentOnlyBytes))
	fmt.Printf("  Snapshot-referenced files:     %d (%.2f MB)\n", r.SnapshotRetention.SnapshotReferencedLogicalFiles, bytesToMB(r.SnapshotRetention.SnapshotReferencedBytes))
	fmt.Printf("  Snapshot-only logical files:   %d (%.2f MB)\n", r.SnapshotRetention.SnapshotOnlyLogicalFiles, bytesToMB(r.SnapshotRetention.SnapshotOnlyBytes))
	fmt.Printf("  Shared logical files:          %d (%.2f MB)\n", r.SnapshotRetention.SharedLogicalFiles, bytesToMB(r.SnapshotRetention.SharedBytes))
	fmt.Printf("File retry stats:                total=%d, avg=%.2f, max=%d\n", r.TotalFileRetries, r.AvgFileRetries, r.MaxFileRetries)
	fmt.Printf("Chunk retry stats:               total=%d, avg=%.2f, max=%d\n", r.TotalChunkRetries, r.AvgChunkRetries, r.MaxChunkRetries)
	fmt.Println("============================")
	fmt.Printf("Chunks (total):           %d\n", r.TotalChunks)
	fmt.Printf("  Completed chunks:       %d (%.2f MB)\n", r.CompletedChunks, bytesToMB(r.CompletedChunkBytes))
	fmt.Printf("  Processing chunks:      %d\n", r.ProcessingChunks)
	fmt.Printf("  Aborted chunks:         %d\n", r.AbortedChunks)
	if len(r.ChunkCountsByVersion) > 0 {
		fmt.Printf("Chunker Distribution:\n")
		versions := make([]string, 0, len(r.ChunkCountsByVersion))
		for version := range r.ChunkCountsByVersion {
			versions = append(versions, version)
		}
		sort.Strings(versions)
		for _, version := range versions {
			fmt.Printf("  %-22s %d chunks\n", version+":", r.ChunkCountsByVersion[version])
		}
	}
	if len(r.ChunkBytesByVersion) > 0 {
		fmt.Printf("Stored Data by Chunker:\n")
		versions := make([]string, 0, len(r.ChunkBytesByVersion))
		for version := range r.ChunkBytesByVersion {
			versions = append(versions, version)
		}
		sort.Strings(versions)
		for _, version := range versions {
			fmt.Printf("  %-22s %.2f GB\n", version+":", bytesToGB(r.ChunkBytesByVersion[version]))
		}
	}
	if len(r.LogicalFileCountsByVersion) > 0 {
		fmt.Printf("Logical Files by Chunker:\n")
		versions := make([]string, 0, len(r.LogicalFileCountsByVersion))
		for version := range r.LogicalFileCountsByVersion {
			versions = append(versions, version)
		}
		sort.Strings(versions)
		for _, version := range versions {
			fmt.Printf("  %-22s %d files\n", version+":", r.LogicalFileCountsByVersion[version])
		}
	}
	if hasMixedRepositoryChunkerVersions(r) {
		fmt.Println("⚠ Repository contains multiple chunker versions.")
		fmt.Println("  This is expected after upgrades or configuration changes.")
	}
	fmt.Printf("Dedup Signal:\n")
	fmt.Printf("  Total chunk references:  %d\n", r.TotalChunkReferences)
	fmt.Printf("  Unique referenced chunks:%d\n", r.UniqueReferencedChunks)
	fmt.Printf("  Estimated dedup ratio:   %.2f%%\n", r.EstimatedDedupRatioPct)
	fmt.Println("============================")
	fmt.Println("\nPer-container breakdown:")
	for _, c := range r.Containers {
		fmt.Printf("Container %d (%s): quarantined=%t : total=%.2fMB live=%.2fMB dead=%.2fMB live_ratio=%.2f%%\n",
			c.ID, c.Filename, c.Quarantine,
			bytesToMB(c.TotalBytes), bytesToMB(c.LiveBytes), bytesToMB(c.DeadBytes),
			c.LiveRatioPct,
		)
	}
}

// runVerifyCommand executes recovered-state verification. The verification phase
// itself is read-only; any corrective mutation happens earlier via automatic
// startup recovery before this function is called. It is not intended to be an
// online checker during active writes, where transient metadata/data divergence
// can produce false positives.
func runVerifyCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) == 0 {
		return usageErrorf("Usage: coldkeep verify <system|file <fileID>> [--standard|--full|--deep]\nDid you mean: coldkeep verify system --standard")
	}

	verifyLevel, err := parseVerifyLevel(parsed)
	if err != nil {
		return err
	}

	target := parsed.positionals[0]
	switch target {
	case "system":
		if len(parsed.positionals) > 2 {
			return usageErrorf("Usage: coldkeep verify system [--standard|--full|--deep]")
		}
		verifyErr := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, 0, verifyLevel)
		if verifyErr != nil {
			return verifyError(verifyErr)
		}
		if outputMode == outputModeText {
			fmt.Printf("Hint: %s\n", doctorOperationalHint)
		}
		return nil
	case "file":
		if len(parsed.positionals) < 2 || len(parsed.positionals) > 3 {
			return usageErrorf("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
		}

		fileID, err := strconv.ParseInt(parsed.positionals[1], 10, 64)
		if err != nil {
			return usageErrorf("Invalid fileID: %v", err)
		}

		verifyErr := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, int(fileID), verifyLevel)
		if verifyErr != nil {
			return verifyError(verifyErr)
		}
		if outputMode == outputModeText {
			fmt.Printf("Hint: %s\n", doctorOperationalHint)
		}
		return nil
	default:
		return usageErrorf("Unknown target for verify: %s (expected 'system' or 'file <fileID>')", target)
	}
}

func querySchemaVersion() (int64, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return 0, fmt.Errorf("connect DB for schema check: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	var version sql.NullInt64
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("query schema_version: %w", err)
	}
	if !version.Valid {
		return 0, errors.New("schema_version table is empty")
	}

	return version.Int64, nil
}

// runDoctorCommand implements the doctor corrective recovery command.
// Doctor is NOT read-only: it runs corrective recovery before verification, and may update
// database metadata (aborting dangling PROCESSING writes, clearing stale sealing
// markers) before any integrity check executes. Running doctor on a fresh
// deployment or after an unclean shutdown is safe and intended.
func runDoctorCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep doctor [--standard|--full|--deep]")
	}

	verifyLevel, err := parseDoctorVerifyLevel(parsed)
	if err != nil {
		return err
	}

	report := doctorReport{
		VerifyLevel: verifyLevelToString(verifyLevel),
	}

	recoveryReport, recoveryErr := doctorRecoveryPhase(container.ContainersDir)
	report.Recovery = recoveryReport
	if recoveryErr != nil {
		report.RecoveryStatus = "error"
		return recoveryError(fmt.Errorf("doctor recovery phase failed: %w", recoveryErr))
	}
	report.RecoveryStatus = "ok"

	schemaVersion, schemaErr := doctorSchemaVersionPhase()
	if schemaErr != nil {
		report.SchemaStatus = "error"
		return fmt.Errorf("doctor schema/version check failed: %w", schemaErr)
	}
	report.SchemaVersion = schemaVersion
	report.SchemaStatus = "ok"

	verifyErr := doctorVerifyPhase(container.ContainersDir, "system", 0, verifyLevel)
	if verifyErr != nil {
		report.VerifyStatus = "error"
		return verifyError(fmt.Errorf("doctor verify phase failed: %w", verifyErr))
	}
	report.VerifyStatus = "ok"

	auditSummary, auditErr := doctorSystemAuditPhase()
	if auditErr != nil {
		return verifyError(fmt.Errorf("doctor audit summary phase failed: %w", auditErr))
	}
	report.physicalAudit = auditSummary.Physical
	report.snapshotAudit = auditSummary.Snapshot

	// Intentional JSON contract (frozen v1.0):
	// - Startup/preflight recovery diagnostics are emitted as stderr events
	//   (`event=startup_recovery`) outside this doctor command payload.
	// - Success: doctor-specific payload emitted to stdout; includes phase statuses,
	//   verify_level, schema_version, and the full recovery counter set under "recovery".
	// - Execution short-circuits by phase on error: recovery -> schema -> verify.
	//   This avoids running expensive later checks once an earlier gate already failed.
	// - Failure: generic CLI error payload on stderr via printCLIError.
	// Doctor does not emit partial doctor data on failure.
	// See doctorReport for the full field list and rationale for including recovery counters.

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "doctor",
			"data":    report,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if outputMode == outputModeText {
		fmt.Print(formatDoctorTextReport(report))
	}

	return nil
}

func formatDoctorTextReport(report doctorReport) string {
	overallStatus := "ok"
	if report.RecoveryStatus != "ok" || report.VerifyStatus != "ok" || report.SchemaStatus != "ok" {
		overallStatus = "error"
	}
	recommendedNextStep := doctorRecommendedNextStep(report, overallStatus)

	var b strings.Builder
	b.WriteString("Doctor health report\n")
	_, _ = fmt.Fprintf(&b, "  Overall status:      %s\n", overallStatus)
	_, _ = fmt.Fprintf(&b, "  Verify level:        %s\n", report.VerifyLevel)
	_, _ = fmt.Fprintf(&b, "  Phase 1 - Recovery:  %s\n", report.RecoveryStatus)
	_, _ = fmt.Fprintf(&b, "  Phase 2 - Verify:    %s\n", report.VerifyStatus)
	if report.SchemaStatus == "ok" {
		_, _ = fmt.Fprintf(&b, "  Phase 3 - Schema:    %s (version=%d)\n", report.SchemaStatus, report.SchemaVersion)
	} else {
		_, _ = fmt.Fprintf(&b, "  Phase 3 - Schema:    %s\n", report.SchemaStatus)
	}
	b.WriteString("  Note: Recovery phase may have modified metadata\n")
	_, _ = fmt.Fprintf(&b, "  Recovery summary: aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d\n",
		report.Recovery.AbortedLogicalFiles,
		report.Recovery.AbortedChunks,
		report.Recovery.QuarantinedMissing,
		report.Recovery.QuarantinedCorruptTail,
		report.Recovery.QuarantinedOrphan,
	)
	_, _ = fmt.Fprintf(&b, "  Physical mapping integrity: orphan_physical_file_rows=%d logical_ref_count_mismatches=%d negative_logical_ref_count_rows=%d\n",
		report.physicalAudit.OrphanPhysicalFileRows,
		report.physicalAudit.LogicalRefCountMismatches,
		report.physicalAudit.NegativeLogicalRefCounts,
	)
	_, _ = fmt.Fprintf(&b, "  Snapshot retention integrity: snapshot_file_rows=%d snapshot_referenced_logical_files=%d snapshot_only_logical_files=%d shared_logical_files=%d orphan_snapshot_logical_refs=%d invalid_snapshot_lifecycle_states=%d retained_missing_chunk_graph=%d\n",
		report.snapshotAudit.SnapshotFileRows,
		report.snapshotAudit.SnapshotReferencedLogicalFiles,
		report.snapshotAudit.SnapshotOnlyLogicalFiles,
		report.snapshotAudit.SharedLogicalFiles,
		report.snapshotAudit.OrphanSnapshotLogicalRefs,
		report.snapshotAudit.InvalidSnapshotLifecycleStates,
		report.snapshotAudit.RetainedMissingChunkGraph,
	)
	_, _ = fmt.Fprintf(&b, "  Recommended next step: %s\n", recommendedNextStep)

	return b.String()
}

func doctorRecommendedNextStep(report doctorReport, overallStatus string) string {
	if overallStatus != "ok" {
		return "inspect stderr / doctor output"
	}

	if report.VerifyLevel == verifyLevelToString(verify.VerifyStandard) {
		return "run doctor --full"
	}

	return "none"
}

func parseDoctorVerifyLevel(parsed parsedCommandLine) (verify.VerifyLevel, error) {
	if !parsed.hasFlag("standard", "full", "deep") {
		return doctorDefaultVerifyLevel, nil
	}

	return parseVerifyLevel(parsed)
}

// SimulateReport holds the result of a dry-run simulation.
type SimulateReport struct {
	Subcommand        string  `json:"subcommand"`
	Path              string  `json:"path"`
	Files             int64   `json:"files"`
	Chunks            int64   `json:"chunks"`
	Containers        int64   `json:"containers"`
	LogicalSizeBytes  int64   `json:"logical_size_bytes"`
	PhysicalSizeBytes int64   `json:"physical_size_bytes"`
	DedupRatioPct     float64 `json:"dedup_ratio_pct"`
}

// BenchmarkChunkersReport is the deterministic output payload for
// `coldkeep benchmark chunkers`.
type BenchmarkChunkersReport struct {
	GeneratedAtUTC string                          `json:"generated_at_utc"`
	Rows           []BenchmarkChunkersReportRecord `json:"rows"`
}

// BenchmarkChunkersReportRecord is one dataset-level comparison row.
type BenchmarkChunkersReportRecord struct {
	Dataset       string  `json:"dataset"`
	Metric        string  `json:"metric"`
	V1SimplePct   float64 `json:"v1_simple_pct"`
	V2FastCDCPct  float64 `json:"v2_fastcdc_pct"`
	DeltaPct      float64 `json:"delta_pct"`
	WinnerVersion string  `json:"winner_version"`
}

func runBenchmarkCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep benchmark <chunkers> [--output <text|json>]")
	}
	if len(parsed.positionals) > 1 {
		return usageErrorf("unexpected benchmark arguments: %s", strings.Join(parsed.positionals[1:], " "))
	}

	subcommand := parsed.positionals[0]
	if subcommand != "chunkers" {
		return usageErrorf("unknown benchmark subcommand %q (expected: chunkers)", subcommand)
	}

	report, err := runChunkerBenchmarkPhase()
	if err != nil {
		return fmt.Errorf("benchmark chunkers: %w", err)
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "benchmark",
			"data":    report,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Println("Chunker benchmark (deterministic synthetic datasets)")
	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "DATASET\tMETRIC\tV1 SIMPLE (%)\tV2 FASTCDC (%)\tDELTA (%)\tWINNER")
	for _, row := range report.Rows {
		_, _ = fmt.Fprintf(
			tw,
			"%s\t%s\t%.2f\t%.2f\t%.2f\t%s\n",
			row.Dataset,
			row.Metric,
			row.V1SimplePct,
			row.V2FastCDCPct,
			row.DeltaPct,
			row.WinnerVersion,
		)
	}
	_ = tw.Flush()

	fmt.Println()
	fmt.Println("Typical outcomes (informational):")
	fmt.Println("  Small modifications: v1 ~92-96% reuse, v2 ~94-98% reuse")
	fmt.Println("  Shifted data:        v1 ~5-20% reuse,  v2 ~25-50% reuse")
	fmt.Println("  The shifted-data gap is the key justification signal.")
	fmt.Println("  FastCDC is designed to improve dedup stability over time; actual results depend on workload.")

	return nil
}

func runChunkerBenchmark() (BenchmarkChunkersReport, error) {
	type metricSpec struct {
		datasetName string
		metricName  string
		compute     func(base, candidate benchmark.Result) (float64, error)
	}

	metrics := []metricSpec{
		{
			datasetName: "slight-modifications",
			metricName:  "reuse-after-small-edit",
			compute: func(base, candidate benchmark.Result) (float64, error) {
				reuse, err := benchmark.CompareReuse(base, candidate)
				if err != nil {
					return 0, err
				}
				return reuse.ReuseRatioPct, nil
			},
		},
		{
			datasetName: "shifted-data",
			metricName:  "reuse-after-shift",
			compute: func(base, candidate benchmark.Result) (float64, error) {
				stability, err := benchmark.CompareBoundaryStability(base, candidate)
				if err != nil {
					return 0, err
				}
				return stability.ReuseAfterShiftPct, nil
			},
		},
	}

	index := make(map[string]benchmark.Dataset)
	for _, dataset := range benchmark.DefaultDatasets() {
		index[dataset.Name] = dataset
	}

	v1 := simplecdc.New()
	v2 := fastcdc.New()

	rows := make([]BenchmarkChunkersReportRecord, 0, len(metrics))
	for _, spec := range metrics {
		dataset, ok := index[spec.datasetName]
		if !ok {
			return BenchmarkChunkersReport{}, fmt.Errorf("missing benchmark dataset %q", spec.datasetName)
		}
		if len(dataset.Mutations) == 0 {
			return BenchmarkChunkersReport{}, fmt.Errorf("benchmark dataset %q has no mutation variants", spec.datasetName)
		}

		baseV1 := benchmark.RunChunker(v1, dataset.Base.Data)
		candidateV1 := benchmark.RunChunker(v1, dataset.Mutations[0].Data)
		if err := benchmark.ValidateCoverageInvariants(int64(len(dataset.Base.Data)), baseV1); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate base coverage for %q v1: %w", spec.datasetName, err)
		}
		if err := benchmark.ValidateCoverageInvariants(int64(len(dataset.Mutations[0].Data)), candidateV1); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate candidate coverage for %q v1: %w", spec.datasetName, err)
		}
		v1Pct, err := spec.compute(baseV1, candidateV1)
		if err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("compute %s for %q v1: %w", spec.metricName, spec.datasetName, err)
		}

		baseV2 := benchmark.RunChunker(v2, dataset.Base.Data)
		candidateV2 := benchmark.RunChunker(v2, dataset.Mutations[0].Data)
		if err := benchmark.ValidateCoverageInvariants(int64(len(dataset.Base.Data)), baseV2); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate base coverage for %q v2: %w", spec.datasetName, err)
		}
		if err := benchmark.ValidateCoverageInvariants(int64(len(dataset.Mutations[0].Data)), candidateV2); err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("validate candidate coverage for %q v2: %w", spec.datasetName, err)
		}
		v2Pct, err := spec.compute(baseV2, candidateV2)
		if err != nil {
			return BenchmarkChunkersReport{}, fmt.Errorf("compute %s for %q v2: %w", spec.metricName, spec.datasetName, err)
		}

		winner := string(chunk.VersionV2FastCDC)
		if v1Pct > v2Pct {
			winner = string(chunk.VersionV1SimpleRolling)
		}
		if math.Abs(v2Pct-v1Pct) < 0.0001 {
			winner = "tie"
		}

		rows = append(rows, BenchmarkChunkersReportRecord{
			Dataset:       spec.datasetName,
			Metric:        spec.metricName,
			V1SimplePct:   v1Pct,
			V2FastCDCPct:  v2Pct,
			DeltaPct:      v2Pct - v1Pct,
			WinnerVersion: winner,
		})
	}

	return BenchmarkChunkersReport{
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Rows:           rows,
	}, nil
}

func runSimulateCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "codec", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep simulate <store|store-folder> [--codec <codec>] <path>")
	}

	subcommand := parsed.positionals[0]
	path := parsed.positionals[1]
	codecName, _ := parsed.lastFlagValue("codec")

	switch subcommand {
	case "store", "store-folder":
	default:
		return usageErrorf("unknown simulate subcommand %q (expected: store, store-folder)", subcommand)
	}

	var codec blocks.Codec
	if codecName != "" {
		if codecName == "plain" {
			fmt.Fprintln(os.Stderr, "WARNING: data would be stored without encryption")
		}
		var err error
		codec, err = blocks.ParseCodec(codecName)
		if err != nil {
			return err
		}
	}

	sgctx, err := storage.ParseStorageContext("simulated")
	if err != nil {
		return fmt.Errorf("create simulated storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	// Run the simulation with stdout suppressed so that internal progress prints
	// don't appear before the structured simulation report.
	err = suppressStdoutDuring(func() error {
		switch subcommand {
		case "store":
			if codecName == "" {
				return storage.StoreFileWithStorageContext(sgctx, path)
			}
			return storage.StoreFileWithStorageContextAndCodec(sgctx, path, codec)
		case "store-folder":
			if codecName == "" {
				return storage.StoreFolderWithStorageContext(sgctx, path)
			}
			return storage.StoreFolderWithStorageContextAndCodec(sgctx, path, codec)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return emitSimulateReport(sgctx, subcommand, path, outputMode)
}

// suppressStdoutDuring redirects os.Stdout to /dev/null for the duration of fn.
func suppressStdoutDuring(fn func() error) error {
	restore, err := suppressStdout()
	if err != nil {
		return fn()
	}
	defer restore()
	fnErr := fn()
	return fnErr
}

func suppressStdout() (func(), error) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}

	stdoutRedirectMu.Lock()
	old := os.Stdout
	os.Stdout = devNull

	return func() {
		os.Stdout = old
		_ = devNull.Close()
		stdoutRedirectMu.Unlock()
	}, nil
}

func emitSimulateReport(sgctx storage.StorageContext, subcommand, path string, outputMode cliOutputMode) error {
	r := &SimulateReport{
		Subcommand: subcommand,
		Path:       path,
	}
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	queries := []struct {
		dest  interface{}
		query string
		args  []any
	}{
		{&r.Files, `SELECT COUNT(*) FROM logical_file WHERE status = $1`, []any{filestate.LogicalFileCompleted}},
		{&r.LogicalSizeBytes, `SELECT COALESCE(SUM(total_size),0) FROM logical_file WHERE status = $1`, []any{filestate.LogicalFileCompleted}},
		{&r.Chunks, `SELECT COUNT(*) FROM chunk WHERE status = $1`, []any{filestate.ChunkCompleted}},
		{&r.Containers, `SELECT COUNT(DISTINCT b.container_id) FROM blocks b JOIN chunk c ON c.id = b.chunk_id WHERE c.status = $1`, []any{filestate.ChunkCompleted}},
		{&r.PhysicalSizeBytes, `SELECT COALESCE(SUM(b.stored_size),0) FROM blocks b JOIN chunk c ON c.id = b.chunk_id WHERE c.live_ref_count > 0 OR c.pin_count > 0`, nil},
	}
	for _, q := range queries {
		if err := sgctx.DB.QueryRowContext(ctx, q.query, q.args...).Scan(q.dest); err != nil {
			return fmt.Errorf("query simulate stats: %w", err)
		}
	}

	if r.LogicalSizeBytes > 0 {
		r.DedupRatioPct = (1.0 - float64(r.PhysicalSizeBytes)/float64(r.LogicalSizeBytes)) * 100
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":    "ok",
			"command":   "simulate",
			"simulated": true,
			"data":      r,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Printf("[SIMULATE] subcommand=%s path=%s (dry run — no data written to storage)\n", subcommand, path)
	fmt.Printf("  Files:          %d\n", r.Files)
	fmt.Printf("  Chunks:         %d\n", r.Chunks)
	fmt.Printf("  Containers:     %d\n", r.Containers)
	fmt.Printf("  Logical size:   %d bytes (%.2f MB)\n", r.LogicalSizeBytes, float64(r.LogicalSizeBytes)/(1024*1024))
	fmt.Printf("  Physical size:  %d bytes (%.2f MB)\n", r.PhysicalSizeBytes, float64(r.PhysicalSizeBytes)/(1024*1024))
	if r.DedupRatioPct > 0 {
		fmt.Printf("  Dedup savings:  %.2f%%\n", r.DedupRatioPct)
	}
	return nil
}

func generateSnapshotID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate snapshot id entropy: %w", err)
	}
	return "snap-" + hex.EncodeToString(b), nil
}

func runSnapshotCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep snapshot <create|restore|list|show|stats|delete|diff> ...")
	}

	subcommand := strings.TrimSpace(strings.ToLower(parsed.positionals[0]))
	switch subcommand {
	case "create":
		return runSnapshotCreateCommand(parsed, outputMode)
	case "restore":
		return runSnapshotRestoreCommand(parsed, outputMode)
	case "list":
		return runSnapshotListCommand(parsed, outputMode)
	case "show":
		return runSnapshotShowCommand(parsed, outputMode)
	case "stats":
		return runSnapshotStatsCommand(parsed, outputMode)
	case "delete":
		return runSnapshotDeleteCommand(parsed, outputMode)
	case "diff":
		return runSnapshotDiffCommand(parsed, outputMode)
	default:
		return usageErrorf("unknown snapshot subcommand: %s", parsed.positionals[0])
	}
}

func parseSnapshotDateFlag(flagName, value string, endOfDay bool) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, usageErrorf("--%s cannot be empty", flagName)
	}

	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		utc := parsed.UTC()
		return &utc, nil
	}

	parsedDate, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return nil, usageErrorf("invalid --%s value %q: use RFC3339 or YYYY-MM-DD", flagName, value)
	}
	if endOfDay {
		parsedDate = parsedDate.UTC().Add(24*time.Hour - time.Nanosecond)
	} else {
		parsedDate = parsedDate.UTC()
	}
	return &parsedDate, nil
}

func loadSnapshotDB() (storage.StorageContext, error) {
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return storage.StorageContext{}, fmt.Errorf("load storage context: %w", err)
	}
	if sgctx.DB == nil {
		_ = sgctx.Close()
		return storage.StorageContext{}, errors.New("storage context DB is nil")
	}
	return sgctx, nil
}

// parseSnapshotQuery builds a SnapshotQuery from the query-related flags in parsed.
// Returns nil if no query flags are set. Recognized flags:
// --path, --prefix, --pattern, --regex, --min-size, --max-size,
// --modified-after, --modified-before.
func parseSnapshotQuery(parsed parsedCommandLine) (*snapshot.SnapshotQuery, error) {
	q := &snapshot.SnapshotQuery{}
	hasAny := false

	if values := parsed.flagValues("path"); len(values) > 0 {
		q.ExactPaths = make(map[string]struct{}, len(values))
		for _, value := range values {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return nil, usageErrorf("--path cannot be empty")
			}
			normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
			if err != nil {
				return nil, usageErrorf("invalid --path value %q: %v", trimmed, err)
			}
			q.ExactPaths[normalized] = struct{}{}
		}
		hasAny = true
	}

	if values := parsed.flagValues("prefix"); len(values) > 0 {
		q.Prefixes = make([]string, 0, len(values))
		for _, value := range values {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return nil, usageErrorf("--prefix cannot be empty")
			}
			normalized, err := snapshot.NormalizeSnapshotPath(trimmed)
			if err != nil {
				return nil, usageErrorf("invalid --prefix value %q: %v", trimmed, err)
			}
			if !strings.HasSuffix(normalized, "/") {
				return nil, usageErrorf("invalid --prefix value %q: must end with '/'", trimmed)
			}
			q.Prefixes = append(q.Prefixes, normalized)
		}
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("pattern"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, usageErrorf("--pattern cannot be empty")
		}
		// Validate the glob syntax early so users get a clear error rather than
		// a silent empty-result caused by path.ErrBadPattern at match time.
		if _, err := path.Match(trimmed, ""); err != nil {
			return nil, usageErrorf("invalid --pattern value %q: %v", trimmed, err)
		}
		q.Pattern = trimmed
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("regex"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, usageErrorf("--regex cannot be empty")
		}
		compiled, err := regexp.Compile(trimmed)
		if err != nil {
			return nil, usageErrorf("invalid --regex value %q: %v", trimmed, err)
		}
		q.Regex = compiled
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("min-size"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return nil, usageErrorf("invalid --min-size value %q: must be a non-negative integer", value)
		}
		q.MinSize = &n
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("max-size"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return nil, usageErrorf("invalid --max-size value %q: must be a non-negative integer", value)
		}
		q.MaxSize = &n
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("modified-after"); ok {
		parsedTime, err := parseSnapshotDateFlag("modified-after", value, false)
		if err != nil {
			return nil, err
		}
		q.ModifiedAfter = parsedTime
		hasAny = true
	}

	if value, ok := parsed.lastFlagValue("modified-before"); ok {
		parsedTime, err := parseSnapshotDateFlag("modified-before", value, true)
		if err != nil {
			return nil, err
		}
		q.ModifiedBefore = parsedTime
		hasAny = true
	}

	if q.MinSize != nil && q.MaxSize != nil && *q.MinSize > *q.MaxSize {
		return nil, usageErrorf("--min-size must be <= --max-size")
	}
	if q.ModifiedAfter != nil && q.ModifiedBefore != nil && q.ModifiedAfter.After(*q.ModifiedBefore) {
		return nil, usageErrorf("--modified-after must be <= --modified-before")
	}

	if !hasAny {
		return nil, nil
	}
	return q, nil
}

func snapshotLabelJSONValue(label sql.NullString) any {
	if !label.Valid {
		return nil
	}
	return label.String
}

func snapshotTimeJSONValue(value sql.NullTime) any {
	if !value.Valid {
		return nil
	}
	return value.Time.UTC().Format(time.RFC3339)
}

func snapshotIntJSONValue(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func snapshotSummaryJSON(item snapshot.Snapshot) map[string]any {
	return map[string]any{
		"id":         item.ID,
		"type":       item.Type,
		"created_at": item.CreatedAt.UTC().Format(time.RFC3339),
		"label":      snapshotLabelJSONValue(item.Label),
		"parent_id":  snapshotLabelJSONValue(item.ParentID),
	}
}

func snapshotSortAscending(items []snapshot.Snapshot) []snapshot.Snapshot {
	if len(items) < 2 {
		return append([]snapshot.Snapshot(nil), items...)
	}

	isAscending := true
	isDescending := true
	for i := 1; i < len(items); i++ {
		prev := items[i-1]
		curr := items[i]

		prevBeforeCurr := prev.CreatedAt.Before(curr.CreatedAt) || (prev.CreatedAt.Equal(curr.CreatedAt) && prev.ID < curr.ID)
		currBeforePrev := curr.CreatedAt.Before(prev.CreatedAt) || (prev.CreatedAt.Equal(curr.CreatedAt) && curr.ID < prev.ID)

		if !prevBeforeCurr {
			isAscending = false
		}
		if !currBeforePrev {
			isDescending = false
		}
		if !isAscending && !isDescending {
			break
		}
	}

	if isAscending {
		return append([]snapshot.Snapshot(nil), items...)
	}
	if isDescending {
		ordered := append([]snapshot.Snapshot(nil), items...)
		for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
			ordered[i], ordered[j] = ordered[j], ordered[i]
		}
		return ordered
	}

	ordered := append([]snapshot.Snapshot(nil), items...)
	// Defensive fallback for unsorted/custom inputs; normal list flow should not hit this.
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})
	return ordered
}

func renderSnapshotTreeLines(items []snapshot.Snapshot) []string {
	if len(items) == 0 {
		return nil
	}

	// Tree visualization is derived only from snapshot metadata (id, parent_id,
	// created_at). Missing/NULL parent links are treated as roots so rendering
	// stays resilient after parent deletion or lineage cleanup.
	ordered := snapshotSortAscending(items)
	byID := make(map[string]snapshot.Snapshot, len(ordered))
	children := make(map[string][]snapshot.Snapshot)
	roots := make([]snapshot.Snapshot, 0)
	for _, item := range ordered {
		byID[item.ID] = item
	}
	for _, item := range ordered {
		if item.ParentID.Valid {
			if _, ok := byID[item.ParentID.String]; ok {
				children[item.ParentID.String] = append(children[item.ParentID.String], item)
				continue
			}
		}
		roots = append(roots, item)
	}

	lines := make([]string, 0, len(ordered))
	visited := make(map[string]struct{}, len(ordered))
	var walk func(node snapshot.Snapshot, prefix string, isLast bool, hasParent bool)
	walk = func(node snapshot.Snapshot, prefix string, isLast bool, hasParent bool) {
		if _, seen := visited[node.ID]; seen {
			return
		}
		visited[node.ID] = struct{}{}

		linePrefix := prefix
		if hasParent {
			if isLast {
				linePrefix += "└── "
			} else {
				linePrefix += "├── "
			}
		}
		lines = append(lines, linePrefix+node.ID)

		nextPrefix := prefix
		if hasParent {
			if isLast {
				nextPrefix += "    "
			} else {
				nextPrefix += "│   "
			}
		}

		childItems := children[node.ID]
		for idx, child := range childItems {
			if _, seen := visited[child.ID]; seen {
				log.Printf("WARNING: snapshot tree cycle detected; skipping edge parent=%s child=%s", node.ID, child.ID)
				continue
			}
			walk(child, nextPrefix, idx == len(childItems)-1, true)
		}
	}

	emitTopLevel := func(node snapshot.Snapshot) {
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		walk(node, "", true, false)
	}

	for idx, root := range roots {
		_ = idx
		emitTopLevel(root)
	}
	for _, item := range ordered {
		if _, seen := visited[item.ID]; !seen {
			emitTopLevel(item)
		}
	}
	return lines
}

func snapshotFilesJSON(items []snapshot.SnapshotFileEntry) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{
			"path":            item.Path,
			"logical_file_id": item.LogicalFileID,
			"size":            snapshotIntJSONValue(item.Size),
			"mode":            snapshotIntJSONValue(item.Mode),
			"mtime":           snapshotTimeJSONValue(item.MTime),
		})
	}
	return result
}

func runSnapshotListCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "type", "label", "since", "until", "limit", "tree", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep snapshot list [--type <full|partial>] [--label <substring>] [--since <RFC3339|YYYY-MM-DD>] [--until <RFC3339|YYYY-MM-DD>] [--limit <count>] [--tree] [--output <text|json>]")
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}
	treeMode := parsed.hasFlag("tree")

	filter := snapshot.SnapshotListFilter{}
	if value, ok := parsed.lastFlagValue("type"); ok {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed != "full" && trimmed != "partial" {
			return usageErrorf("invalid --type value %q (allowed: full, partial)", value)
		}
		filter.Type = &trimmed
	}
	if value, ok := parsed.lastFlagValue("label"); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return usageErrorf("--label cannot be empty")
		}
		filter.Label = &trimmed
	}
	if value, ok := parsed.lastFlagValue("since"); ok {
		parsedTime, err := parseSnapshotDateFlag("since", value, false)
		if err != nil {
			return err
		}
		filter.Since = parsedTime
	}
	if value, ok := parsed.lastFlagValue("until"); ok {
		parsedTime, err := parseSnapshotDateFlag("until", value, true)
		if err != nil {
			return err
		}
		filter.Until = parsedTime
	}
	if filter.Since != nil && filter.Until != nil && filter.Since.After(*filter.Until) {
		return usageErrorf("--since must be <= --until")
	}
	if value, ok := parsed.lastFlagValue("limit"); ok {
		parsedLimit, _ := strconv.Atoi(value)
		filter.Limit = parsedLimit
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	items, err := listSnapshotsPhase(ctx, sgctx.DB, filter)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		jsonItems := make([]map[string]any, 0, len(items))
		for _, item := range items {
			jsonItems = append(jsonItems, snapshotSummaryJSON(item))
		}
		data := map[string]any{
			"action":      "list",
			"count":       len(items),
			"duration_ms": time.Since(startedAt).Milliseconds(),
			"snapshots":   jsonItems,
		}
		if treeMode {
			data["tree_mode"] = true
			data["tree_lines"] = renderSnapshotTreeLines(items)
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if treeMode {
		lines := renderSnapshotTreeLines(items)
		if len(lines) == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "no snapshots found")
		} else {
			for _, line := range lines {
				_, _ = fmt.Fprintln(os.Stdout, line)
			}
		}
	} else {
		_, _ = fmt.Fprintln(os.Stdout, "Snapshots:")
		if len(items) == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "  (none)")
		} else {
			for _, item := range items {
				label := ""
				if item.Label.Valid {
					label = "  label=" + item.Label.String
				}
				_, _ = fmt.Fprintf(os.Stdout, "  %s  %s  %s%s\n", item.ID, item.Type, item.CreatedAt.UTC().Format(time.RFC3339), label)
			}
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", time.Since(startedAt).Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runSnapshotShowCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "limit", "output", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) != 2 {
		return usageErrorf("Usage: coldkeep snapshot show <snapshotID> [--limit <count>] [--path <path>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}
	if err := validateNonNegativeIntegerFlag(parsed, "limit"); err != nil {
		return err
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return usageErrorf("snapshotID cannot be empty")
	}
	limit := 0
	if value, ok := parsed.lastFlagValue("limit"); ok {
		limit, _ = strconv.Atoi(value)
	}

	query, err := parseSnapshotQuery(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	item, err := getSnapshotPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}
	files, err := listSnapshotFilesPhase(ctx, sgctx.DB, snapshotID, limit, query)
	if err != nil {
		return err
	}
	stats, err := snapshotStatsPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}
	matchedFileCount := len(files)

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data": map[string]any{
				"action":                    "show",
				"snapshot":                  snapshotSummaryJSON(*item),
				"file_count":                matchedFileCount,
				"matched_file_count":        matchedFileCount,
				"total_snapshot_file_count": stats.SnapshotFileCount,
				"files":                     snapshotFilesJSON(files),
				"duration_ms":               time.Since(startedAt).Milliseconds(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	_, _ = fmt.Fprintf(os.Stdout, "Snapshot: %s\n", item.ID)
	_, _ = fmt.Fprintf(os.Stdout, "  Type: %s\n", item.Type)
	_, _ = fmt.Fprintf(os.Stdout, "  Created: %s\n", item.CreatedAt.UTC().Format(time.RFC3339))
	if item.Label.Valid {
		_, _ = fmt.Fprintf(os.Stdout, "  Label: %s\n", item.Label.String)
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Files (matched): %d\n", matchedFileCount)
	_, _ = fmt.Fprintf(os.Stdout, "  Files (total): %d\n", stats.SnapshotFileCount)
	_, _ = fmt.Fprintln(os.Stdout)
	if len(files) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "  (no files)")
	} else {
		for _, file := range files {
			_, _ = fmt.Fprintf(os.Stdout, "  %s\n", file.Path)
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "\n  Duration: %dms\n", time.Since(startedAt).Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runSnapshotStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) > 2 {
		return usageErrorf("Usage: coldkeep snapshot stats [<snapshotID>] [--output <text|json>]")
	}

	snapshotID := ""
	if len(parsed.positionals) == 2 {
		snapshotID = strings.TrimSpace(parsed.positionals[1])
		if snapshotID == "" {
			return usageErrorf("snapshotID cannot be empty")
		}
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	stats, err := snapshotStatsPhase(ctx, sgctx.DB, snapshotID)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		data := map[string]any{
			"action":              "stats",
			"snapshot_id":         snapshotID,
			"snapshot_count":      stats.SnapshotCount,
			"snapshot_file_count": stats.SnapshotFileCount,
			"total_size_bytes":    stats.TotalSizeBytes,
			"duration_ms":         time.Since(startedAt).Milliseconds(),
		}
		if stats.ParentSnapshotID.Valid {
			if stats.ReusedFileCount.Valid {
				data["reused"] = stats.ReusedFileCount.Int64
			}
			if stats.NewFileCount.Valid {
				data["new"] = stats.NewFileCount.Int64
			}
			if stats.ReuseRatioPct.Valid {
				data["reuse_ratio"] = stats.ReuseRatioPct.Float64
			}
		}

		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if snapshotID == "" {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshots: %d\n", stats.SnapshotCount)
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot files: %d\n", stats.SnapshotFileCount)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot: %s\n", snapshotID)
		_, _ = fmt.Fprintf(os.Stdout, "  Files: %d\n", stats.SnapshotFileCount)
		if stats.ParentSnapshotID.Valid {
			_, _ = fmt.Fprintf(os.Stdout, "  Reused: %d\n", stats.ReusedFileCount.Int64)
			_, _ = fmt.Fprintf(os.Stdout, "  New: %d\n", stats.NewFileCount.Int64)
			_, _ = fmt.Fprintf(os.Stdout, "  Reuse ratio: %.1f%%\n", stats.ReuseRatioPct.Float64)
		} else {
			_, _ = fmt.Fprintf(os.Stdout, "  (%s)\n", snapshotLineageUnavailableMessage(stats.LineageStatus))
		}
	}
	_, _ = fmt.Fprintf(os.Stdout, "Total logical size: %d bytes\n", stats.TotalSizeBytes)
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", time.Since(startedAt).Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func snapshotLineageUnavailableMessage(status snapshot.SnapshotLineageStatus) string {
	switch status {
	case snapshot.SnapshotLineageStatusNoParent, "":
		return "Reused/New not available -- no parent snapshot metadata"
	case snapshot.SnapshotLineageStatusParentMissing:
		return "Reused/New not available -- parent snapshot metadata exists but parent snapshot is missing"
	case snapshot.SnapshotLineageStatusSkipped:
		return "Reused/New not available -- lineage analysis was intentionally skipped for this snapshot scope"
	default:
		return "Reused/New not available"
	}
}

func runSnapshotDeleteCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "force", "dry-run", "dryRun", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 2 {
		return usageErrorf("Usage: coldkeep snapshot delete <snapshotID> (--force|--dry-run) [--output <text|json>]")
	}
	dryRun := parsed.hasFlag("dry-run", "dryRun")
	if !dryRun && !parsed.hasFlag("force") {
		return usageErrorf("snapshot delete requires --force or --dry-run")
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return usageErrorf("snapshotID cannot be empty")
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	var preview *snapshotDeleteLineagePreview
	if dryRun {
		loadedPreview, err := snapshotDeleteLineagePreviewPhase(ctx, sgctx.DB, snapshotID)
		if err != nil {
			return err
		}
		preview = loadedPreview
	}

	if !dryRun {
		if err := deleteSnapshotPhase(ctx, sgctx.DB, snapshotID); err != nil {
			return err
		}
	}

	if outputMode == outputModeJSON {
		action := "delete"
		if dryRun {
			action = "delete_dry_run"
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data": map[string]any{
				"action":         action,
				"snapshot_id":    snapshotID,
				"dry_run":        dryRun,
				"parent_id":      snapshotLabelJSONValue(previewParentID(preview)),
				"parent_missing": previewParentMissing(preview),
				"children":       previewChildren(preview),
				"total_files":    previewTotalFiles(preview),
				"unique_files":   previewUniqueFiles(preview),
				"shared_files":   previewSharedFiles(preview),
				"warnings":       previewWarnings(preview),
				"duration_ms":    time.Since(startedAt).Milliseconds(),
			},
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if dryRun {
		output := formatSnapshotDeleteDryRunOutput(snapshotID, preview)
		_, _ = fmt.Fprint(os.Stdout, output)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot deleted: id=%s\n", snapshotID)
		_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", time.Since(startedAt).Milliseconds())
		_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	}
	return nil
}

type snapshotDeleteLineagePreview struct {
	SnapshotID       string
	ParentID         sql.NullString
	ParentMissing    bool
	ChildSnapshotIDs []string
	TotalFiles       int64
	UniqueFiles      int64
	SharedFiles      int64
}

func loadSnapshotDeleteLineagePreview(ctx context.Context, dbconn *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
	if dbconn == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	trimmedID := strings.TrimSpace(snapshotID)
	if trimmedID == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}

	preview := &snapshotDeleteLineagePreview{}
	if err := dbconn.QueryRowContext(ctx, `SELECT id, parent_id FROM snapshot WHERE id = $1`, trimmedID).Scan(&preview.SnapshotID, &preview.ParentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("snapshot %q not found", trimmedID)
		}
		return nil, fmt.Errorf("load snapshot delete preview snapshot_id=%s: %w", trimmedID, err)
	}
	if preview.ParentID.Valid {
		var parentExists bool
		if err := dbconn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM snapshot WHERE id = $1)`, preview.ParentID.String).Scan(&parentExists); err != nil {
			return nil, fmt.Errorf("load snapshot delete preview parent existence snapshot_id=%s parent_id=%s: %w", trimmedID, preview.ParentID.String, err)
		}
		preview.ParentMissing = !parentExists
	}

	rows, err := dbconn.QueryContext(ctx, `SELECT id FROM snapshot WHERE parent_id = $1 ORDER BY id`, trimmedID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot delete preview children snapshot_id=%s: %w", trimmedID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var childID string
		if err := rows.Scan(&childID); err != nil {
			return nil, fmt.Errorf("scan snapshot delete preview child row snapshot_id=%s: %w", trimmedID, err)
		}
		preview.ChildSnapshotIDs = append(preview.ChildSnapshotIDs, childID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot delete preview child rows snapshot_id=%s: %w", trimmedID, err)
	}

	// Query total files referenced by this snapshot
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, trimmedID).Scan(&preview.TotalFiles); err != nil {
		return nil, fmt.Errorf("load snapshot delete preview count total files snapshot_id=%s: %w", trimmedID, err)
	}

	// Query unique files (only referenced by this snapshot).
	// A file is shared only when BOTH path_id and logical_file_id match another snapshot row.
	// Same path_id with different logical_file_id is intentionally treated as unique.
	//
	// Performance note:
	// - This NOT EXISTS anti-join relies on snapshot_file indexes for acceptable latency:
	//   idx_snapshot_file_unique (snapshot_id, path_id), idx_snapshot_file_path_id (path_id),
	//   and idx_snapshot_file_logical_file (logical_file_id).
	// - This runs in a dry-run operator path for cold-storage workflows where snapshot counts are
	//   expected to remain much smaller than file counts, so current cost is acceptable.
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM snapshot_file sf
		WHERE sf.snapshot_id = $1
		AND NOT EXISTS (
			SELECT 1
			FROM snapshot_file sf2
			WHERE sf2.path_id = sf.path_id
			  AND sf2.logical_file_id = sf.logical_file_id
			  AND sf2.snapshot_id != sf.snapshot_id
		)
	`, trimmedID).Scan(&preview.UniqueFiles); err != nil {
		return nil, fmt.Errorf("load snapshot delete preview count unique files snapshot_id=%s: %w", trimmedID, err)
	}

	// Calculate shared files
	preview.SharedFiles = preview.TotalFiles - preview.UniqueFiles

	return preview, nil
}

func previewParentID(preview *snapshotDeleteLineagePreview) sql.NullString {
	if preview == nil {
		return sql.NullString{}
	}
	return preview.ParentID
}

func previewParentMissing(preview *snapshotDeleteLineagePreview) bool {
	if preview == nil {
		return false
	}
	return preview.ParentMissing
}

func previewChildren(preview *snapshotDeleteLineagePreview) []string {
	if preview == nil {
		return nil
	}
	return append([]string(nil), preview.ChildSnapshotIDs...)
}

func previewTotalFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.TotalFiles
}

func previewUniqueFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.UniqueFiles
}

func previewSharedFiles(preview *snapshotDeleteLineagePreview) int64 {
	if preview == nil {
		return 0
	}
	return preview.SharedFiles
}

// formatNumberWithCommas formats a number int64 with comma separators for readability
func formatNumberWithCommas(n int64) string {
	if n < 0 {
		return "-" + formatNumberWithCommas(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

func previewWarnings(preview *snapshotDeleteLineagePreview) []map[string]any {
	if preview == nil || len(preview.ChildSnapshotIDs) == 0 {
		return nil
	}
	return []map[string]any{
		{
			"type":    "lineage_breakage",
			"message": "Deleting this snapshot will break lineage visualization for its children.",
			"details": map[string]any{
				"affected_snapshots": preview.ChildSnapshotIDs,
				"note":               "Affected snapshots remain fully usable; only lineage information is affected.",
			},
		},
	}
}

// formatSnapshotDeleteDryRunOutput builds the formatted text output for dry-run delete
func formatSnapshotDeleteDryRunOutput(snapshotID string, preview *snapshotDeleteLineagePreview) string {
	var buf strings.Builder

	// Header
	fmt.Fprintf(&buf, "Snapshot: %s\n", snapshotID)
	buf.WriteString("\n")

	if preview != nil {
		// Files section
		buf.WriteString("Files:\n")
		fmt.Fprintf(&buf, "  Total:        %s\n", formatNumberWithCommas(preview.TotalFiles))
		fmt.Fprintf(&buf, "  Unique:       %s\n", formatNumberWithCommas(preview.UniqueFiles))
		fmt.Fprintf(&buf, "  Shared:       %s\n", formatNumberWithCommas(preview.SharedFiles))
		buf.WriteString("\n")

		// Lineage section
		buf.WriteString("Lineage:\n")
		if preview.ParentID.Valid {
			if preview.ParentMissing {
				buf.WriteString("  Parent: (missing)\n")
				buf.WriteString("  Parent note: parent snapshot metadata is missing; this snapshot remains usable\n")
			} else {
				fmt.Fprintf(&buf, "  Parent: %s\n", preview.ParentID.String)
			}
		} else {
			buf.WriteString("  Parent: none\n")
		}
		if len(preview.ChildSnapshotIDs) > 0 {
			buf.WriteString("  Children:\n")
			for _, childID := range preview.ChildSnapshotIDs {
				fmt.Fprintf(&buf, "    - %s\n", childID)
			}
		} else {
			buf.WriteString("  Children: none\n")
		}
		buf.WriteString("\n")

		// Warning section (only if has children)
		if len(preview.ChildSnapshotIDs) > 0 {
			buf.WriteString("Warning:\n")
			buf.WriteString("  This snapshot is parent of:\n")
			for _, childID := range preview.ChildSnapshotIDs {
				fmt.Fprintf(&buf, "    - %s\n", childID)
			}
			buf.WriteString("\n")
			buf.WriteString("  Deleting it will break lineage visualization,\n")
			buf.WriteString("  but snapshots remain fully usable.\n")
			buf.WriteString("\n")
		}

		// Impact section
		buf.WriteString("Impact:\n")
		buf.WriteString("  Deleting this snapshot will:\n")
		buf.WriteString("    - remove snapshot metadata\n")
		buf.WriteString("    - NOT delete shared data\n")
		if preview.UniqueFiles > 0 {
			fmt.Fprintf(&buf, "    - remove %s unique snapshot file reference(s) from metadata\n", formatNumberWithCommas(preview.UniqueFiles))
			buf.WriteString("      (reference impact only; does not guarantee reclaimed disk space)\n")
		}
		buf.WriteString("\n")
	}

	// Dry-run notice
	buf.WriteString("Dry run: no changes applied.\n")

	return buf.String()
}

func runSnapshotDiffCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "filter", "summary", "output", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) != 3 {
		return usageErrorf("Usage: coldkeep snapshot diff <baseSnapshotID> <targetSnapshotID> [--summary] [--filter <added|removed|modified>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}

	baseID := strings.TrimSpace(parsed.positionals[1])
	targetID := strings.TrimSpace(parsed.positionals[2])
	if baseID == "" {
		return usageErrorf("baseSnapshotID cannot be empty")
	}
	if targetID == "" {
		return usageErrorf("targetSnapshotID cannot be empty")
	}

	filterType := ""
	summaryMode := parsed.hasFlag("summary")
	if value, ok := parsed.lastFlagValue("filter"); ok {
		filterType = strings.ToLower(strings.TrimSpace(value))
		switch filterType {
		case "added", "removed", "modified":
		default:
			return usageErrorf("invalid --filter value %q (allowed: added, removed, modified)", value)
		}
	}

	query, err := parseSnapshotQuery(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadSnapshotDB()
	if err != nil {
		return err
	}
	defer func() { _ = sgctx.Close() }()

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	useSummaryFastPath := summaryMode && filterType == "" && query == nil
	if useSummaryFastPath {
		summary, err := diffSnapshotSummaryPhase(ctx, sgctx.DB, baseID, targetID)
		if err != nil {
			return err
		}
		totalEntryCount := int(summary.Added + summary.Removed + summary.Modified)

		if outputMode == outputModeJSON {
			payload := map[string]any{
				"status":  "ok",
				"command": "snapshot diff",
				"data": map[string]any{
					"base":                   baseID,
					"target":                 targetID,
					"entry_count":            totalEntryCount,
					"matched_entry_count":    totalEntryCount,
					"total_diff_entry_count": totalEntryCount,
					"summary":                summary,
					"summary_mode":           true,
					"duration_ms":            time.Since(startedAt).Milliseconds(),
				},
			}
			encoded, _ := json.Marshal(payload)
			fmt.Println(string(encoded))
			return nil
		}

		_, _ = fmt.Fprintf(os.Stdout, "Snapshot diff: %s -> %s\n\n", baseID, targetID)
		_, _ = fmt.Fprintf(os.Stdout, "Added:     %d files\n", summary.Added)
		_, _ = fmt.Fprintf(os.Stdout, "Removed:   %d files\n", summary.Removed)
		_, _ = fmt.Fprintf(os.Stdout, "Modified:  %d files\n", summary.Modified)
		_, _ = fmt.Fprintf(os.Stdout, "Total changes: %d\n", totalEntryCount)
		return nil
	}

	result, err := diffSnapshotsPhase(ctx, sgctx.DB, baseID, targetID, query)
	if err != nil {
		return err
	}

	entries := make([]snapshot.SnapshotDiffEntry, 0, len(result.Entries))
	summary := snapshot.SnapshotDiffSummary{}
	for _, entry := range result.Entries {
		if filterType != "" && entry.Type != snapshot.DiffType(filterType) {
			continue
		}
		entries = append(entries, entry)
		switch entry.Type {
		case snapshot.DiffAdded:
			summary.Added++
		case snapshot.DiffRemoved:
			summary.Removed++
		case snapshot.DiffModified:
			summary.Modified++
		}
	}
	totalEntryCount := len(result.Entries)
	matchedEntryCount := len(entries)

	if outputMode == outputModeJSON {
		jsonEntries := make([]map[string]any, 0, len(entries))
		if !summaryMode {
			for _, entry := range entries {
				jsonEntries = append(jsonEntries, map[string]any{
					"path":              entry.Path,
					"type":              entry.Type,
					"base_logical_id":   snapshotIntJSONValue(entry.BaseLogicalID),
					"target_logical_id": snapshotIntJSONValue(entry.TargetLogicalID),
				})
			}
		}

		data := map[string]any{
			"base":                   baseID,
			"target":                 targetID,
			"entry_count":            matchedEntryCount,
			"matched_entry_count":    matchedEntryCount,
			"total_diff_entry_count": totalEntryCount,
			"summary":                summary,
			"duration_ms":            time.Since(startedAt).Milliseconds(),
		}
		if summaryMode {
			data["summary_mode"] = true
		} else {
			data["entries"] = jsonEntries
		}

		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot diff",
			"data":    data,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if summaryMode {
		totalChanges := summary.Added + summary.Removed + summary.Modified
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot diff: %s -> %s\n\n", baseID, targetID)
		_, _ = fmt.Fprintf(os.Stdout, "Added:     %d files\n", summary.Added)
		_, _ = fmt.Fprintf(os.Stdout, "Removed:   %d files\n", summary.Removed)
		_, _ = fmt.Fprintf(os.Stdout, "Modified:  %d files\n", summary.Modified)
		_, _ = fmt.Fprintf(os.Stdout, "Total changes: %d\n", totalChanges)
		return nil
	}

	_, _ = fmt.Fprintln(os.Stdout, "[SNAPSHOT DIFF]")
	_, _ = fmt.Fprintf(os.Stdout, "\nBase:    %s\n", baseID)
	_, _ = fmt.Fprintf(os.Stdout, "Target:  %s\n\n", targetID)
	if len(entries) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "(no changes)")
	} else {
		for _, entry := range entries {
			prefix := "?"
			switch entry.Type {
			case snapshot.DiffAdded:
				prefix = "+"
			case snapshot.DiffRemoved:
				prefix = "-"
			case snapshot.DiffModified:
				prefix = "~"
			}
			_, _ = fmt.Fprintf(os.Stdout, "%s %s\n", prefix, entry.Path)
		}
	}

	_, _ = fmt.Fprintln(os.Stdout, "\nSummary:")
	_, _ = fmt.Fprintf(os.Stdout, "  entries (matched): %d\n", matchedEntryCount)
	_, _ = fmt.Fprintf(os.Stdout, "  entries (total): %d\n", totalEntryCount)
	_, _ = fmt.Fprintf(os.Stdout, "  added: %d\n", summary.Added)
	_, _ = fmt.Fprintf(os.Stdout, "  removed: %d\n", summary.Removed)
	_, _ = fmt.Fprintf(os.Stdout, "  modified: %d\n", summary.Modified)
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", time.Since(startedAt).Milliseconds())
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runSnapshotCreateCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "id", "label", "from", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) < 1 {
		return usageErrorf("Usage: coldkeep snapshot create [<path> ...] [--id <snapshotID>] [--label <label>] [--from <snapshotID>] [--output <text|json>]")
	}

	paths := parsed.positionals[1:]
	snapshotType := "full"
	if len(paths) > 0 {
		snapshotType = "partial"
	}

	snapshotID, hasSnapshotID := parsed.lastFlagValue("id")
	snapshotID = strings.TrimSpace(snapshotID)
	if !hasSnapshotID || snapshotID == "" {
		generatedID, err := generateSnapshotID()
		if err != nil {
			return err
		}
		snapshotID = generatedID
	}

	var labelPtr *string
	if label, hasLabel := parsed.lastFlagValue("label"); hasLabel {
		trimmed := strings.TrimSpace(label)
		if trimmed == "" {
			return usageErrorf("--label cannot be empty")
		}
		labelPtr = &trimmed
	}

	var parentIDPtr *string
	if fromID, hasFrom := parsed.lastFlagValue("from"); hasFrom {
		trimmed := strings.TrimSpace(fromID)
		if trimmed == "" {
			return usageErrorf("--from cannot be empty")
		}
		parentIDPtr = &trimmed
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if sgctx.DB == nil {
		return errors.New("storage context DB is nil")
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	if err := createSnapshotPhase(ctx, sgctx.DB, snapshot.SnapshotCreateOptions{
		ID:       snapshotID,
		Type:     snapshotType,
		Label:    labelPtr,
		ParentID: parentIDPtr,
		Paths:    paths,
	}); err != nil {
		return err
	}

	var (
		filesInserted      int64
		hasFilesInserted   bool
		snapshotDurationMS = time.Since(startedAt).Milliseconds()
	)
	if err := sgctx.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, snapshotID).Scan(&filesInserted); err == nil {
		hasFilesInserted = true
	}

	if outputMode == outputModeJSON {
		data := map[string]any{
			"snapshot_id": snapshotID,
			"type":        snapshotType,
			"paths_count": len(paths),
			"duration_ms": snapshotDurationMS,
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		if hasFilesInserted {
			data["files_inserted"] = filesInserted
		}
		if labelPtr != nil {
			data["label"] = *labelPtr
		}
		if parentIDPtr != nil {
			data["parent_id"] = *parentIDPtr
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	if parentIDPtr != nil {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot %q created from parent %q\n", snapshotID, *parentIDPtr)
	} else if snapshotType == "full" {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot created: id=%s type=%s (all paths)\n", snapshotID, snapshotType)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot created: id=%s type=%s paths=%d\n", snapshotID, snapshotType, len(paths))
	}
	if hasFilesInserted {
		_, _ = fmt.Fprintf(os.Stdout, "  Files: %d\n", filesInserted)
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", snapshotDurationMS)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func runSnapshotRestoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	startedAt := time.Now()

	if err := ensureAllowedFlags(parsed, "mode", "destination", "overwrite", "strict", "no-metadata", "output", "path", "prefix", "pattern", "regex", "min-size", "max-size", "modified-after", "modified-before"); err != nil {
		return err
	}
	if len(parsed.positionals) < 2 {
		return usageErrorf("Usage: coldkeep snapshot restore <snapshotID> [<path> ...] [--mode <original|prefix|override>] [--destination <path>] [--overwrite] [--strict] [--no-metadata] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <timestamp>] [--modified-before <timestamp>] [--output <text|json>]")
	}

	snapshotID := strings.TrimSpace(parsed.positionals[1])
	if snapshotID == "" {
		return usageErrorf("snapshotID cannot be empty")
	}
	paths := parsed.positionals[2:]

	strictMetadata := parsed.hasFlag("strict")
	noMetadata := parsed.hasFlag("no-metadata")
	if strictMetadata && noMetadata {
		return usageErrorf("--strict and --no-metadata cannot be used together")
	}

	destinationMode, err := parseRestoreDestinationMode(parsed)
	if err != nil {
		return err
	}
	destination, _ := parsed.lastFlagValue("destination")
	destination = strings.TrimSpace(destination)
	if destinationMode == storage.RestoreDestinationOriginal && destination != "" {
		return usageErrorf("--destination is only supported with --mode prefix or --mode override")
	}
	if (destinationMode == storage.RestoreDestinationPrefix || destinationMode == storage.RestoreDestinationOverride) && destination == "" {
		return usageErrorf("--destination is required with --mode %s", destinationMode)
	}

	query, err := parseSnapshotQuery(parsed)
	if err != nil {
		return err
	}

	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if sgctx.DB == nil {
		return errors.New("storage context DB is nil")
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	result, err := restoreSnapshotPhase(
		ctx,
		sgctx.DB,
		snapshotID,
		paths,
		snapshot.RestoreSnapshotOptions{
			DestinationMode: destinationMode,
			Destination:     destination,
			Overwrite:       parsed.hasFlag("overwrite"),
			StrictMetadata:  strictMetadata,
			NoMetadata:      noMetadata,
			StorageContext:  &sgctx,
			Query:           query,
		},
	)
	if err != nil {
		return err
	}

	durationMS := time.Since(startedAt).Milliseconds()
	actionType := "full"
	if len(paths) > 0 {
		actionType = "partial_restore"
	}

	if outputMode == outputModeJSON {
		data := map[string]any{
			"action":                "restore",
			"snapshot_id":           snapshotID,
			"type":                  actionType,
			"requested_paths_count": len(paths),
			"restored_files":        result.RestoredFiles,
			"duration_ms":           durationMS,
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "snapshot",
			"data":    data,
		}
		if destination != "" {
			data["output_root"] = destination
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	emptyNote := ""
	if result.RestoredFiles == 0 {
		emptyNote = " (empty snapshot selection)"
	}
	if len(paths) == 0 {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot restored: id=%s files=%d%s\n", snapshotID, result.RestoredFiles, emptyNote)
	} else {
		_, _ = fmt.Fprintf(os.Stdout, "Snapshot restored: id=%s requested_paths=%d restored_files=%d%s\n", snapshotID, len(paths), result.RestoredFiles, emptyNote)
	}
	_, _ = fmt.Fprintf(os.Stdout, "  Duration: %dms\n", durationMS)
	_, _ = fmt.Fprintln(os.Stdout, "  Hint: "+doctorOperationalHint)
	return nil
}

func printHelp() {
	fmt.Printf("coldkeep (v%s)\n", version.String())
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	printHelpRows([][2]string{
		{"  init", "Initialize Coldkeep with a new aes-gcm encryption key"},
		{"  config get default-chunker [--output <text|json>]", "Get repository default chunker for new writes"},
		{"  config set default-chunker <value> [--output <text|json>]", "Set repository default chunker for new writes. Affects only new stored data. Existing data is not modified."},
		{"  doctor [--standard|--full|--deep] [--output <text|json>]", "Recommended operator health gate (corrective; may update metadata via recovery before verify; default: --standard)"},
		{"  store [--codec <codec>] <file>", "Store a single file (state-changing)"},
		{"  store-folder [--codec <codec>] <folder>", "Store all files in a folder recursively (state-changing)"},
		{"  restore <fileID> [<fileID> ...] <outputDir> [--input <file>] [--dry-run] [--overwrite] [--fail-fast] [--output <text|json>]", "Restore one or more logical file IDs byte-identically (chunker-version independent)"},
		{"  remove <fileID> [<fileID> ...] [--input <file>] [--dry-run] [--fail-fast] [--output <text|json>]", "Remove one or more logical file IDs (legacy mode)"},
		{"  remove --stored-path <path> [--output <text|json>]", "Remove one current-state physical path mapping"},
		{"  remove --stored-paths <path> [<path> ...] [--input <file>] [--dry-run] [--fail-fast] [--output <text|json>]", "Batch remove physical path mappings in deterministic input order"},
		{"  repair ref-counts [--batch] [--input <file>] [--fail-fast] [--output <text|json>]", "Recompute logical_file.ref_count from physical_file rows (explicit repair)"},
		{"  gc [options]", "Run garbage collection (state-changing unless --dry-run)"},
		{"    (no options)", "Remove unreferenced data"},
		{"    --dry-run", "Show what would be removed without deleting"},
		{"  benchmark chunkers [--output <text|json>]", "Run deterministic chunker comparison benchmark (observational; no repository state changes)"},
		{"  stats [--output <human|text|json>] [--json] [--containers]", "Show repository statistics (read-only); use --containers for opt-in container detail output"},
		{"  inspect file <fileID> [--output <text|json>]", "Inspect one logical file with chunker and chunking summary"},
		{"  verify [target] [fileID] [options]", "Observational layered integrity verification (assumes recovered state; verification phase is read-only; default: --standard)"},
		{"    [target] can be 'system' or 'file'", ""},
		{"    [options] can be '--standard', '--full', or '--deep'", ""},
		{"    no options defaults to '--standard'", ""},
		{"    verify system [options]", "Perform system-wide verification"},
		{"    verify file <fileID> [options]", "Perform verification for specific file"},
		{"  help", "Show this help message"},
		{"  version", "Show version information"},
		{"  list [--limit <count>] [--offset <count>]", "List stored logical files"},
		{"  search [filters] [--limit <count>] [--offset <count>]", "Search files by filters"},
		{"  snapshot create [<path> ...] [--id <snapshotID>] [--label <label>] [--from <snapshotID>] [--output <text|json>]", "Create a full snapshot (no paths) or partial snapshot (with paths)"},
		{"  snapshot restore <snapshotID> [<path> ...] [--mode ...] [--destination <path>] [--overwrite] [--strict] [--no-metadata] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Restore full or partial content from snapshot_file history"},
		{"  snapshot list [--type <full|partial>] [--label <substring>] [--since <RFC3339|YYYY-MM-DD>] [--until <RFC3339|YYYY-MM-DD>] [--limit <count>] [--tree] [--output <text|json>]", "List snapshots with optional filters; use --tree for lineage view"},
		{"  snapshot show <snapshotID> [--limit <count>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Inspect one snapshot and list its files with optional query filters"},
		{"  snapshot stats [<snapshotID>] [--output <text|json>]", "Show global or per-snapshot statistics"},
		{"  snapshot delete <snapshotID> (--force|--dry-run) [--output <text|json>]", "Delete snapshot metadata; --dry-run shows a read-only impact preview"},
		{"  snapshot diff <baseSnapshotID> <targetSnapshotID> [--summary] [--filter <added|removed|modified>] [--path <exact>] [--prefix <dir/>] [--pattern <glob>] [--regex <re>] [--min-size <bytes>] [--max-size <bytes>] [--modified-after <ts>] [--modified-before <ts>] [--output <text|json>]", "Compare snapshots by path and logical_file_id; --summary returns counts only"},
		{"  simulate <store|store-folder> <path>", "Dry-run store estimate without writing to storage (not proof of physical durability)"},
	})
	fmt.Println("    Filters:")
	fmt.Println("      --name <substring>")
	fmt.Println("      --min-size <bytes>")
	fmt.Println("      --max-size <bytes>")
	fmt.Println("      --limit <count>")
	fmt.Println("      --offset <count>")
	fmt.Println("    Snapshot path matching:")
	fmt.Println("      exact path: snapshot create docs/file.txt")
	fmt.Println("      directory prefix: snapshot create docs/")
	fmt.Println("    Snapshot identity:")
	fmt.Println("      snapshot_id is the system identifier (set via --id on create)")
	fmt.Println("      pass snapshot_id positionally to show/restore/stats/diff/delete")
	fmt.Println("      --label is optional metadata only and is never used for targeting")
	fmt.Println("      --from records lineage metadata only; it does not make a snapshot depend on its parent")
	fmt.Println("      --tree renders metadata lineage only")
	fmt.Println("      --summary returns count-only diff output (no entry list)")
	fmt.Println("      --dry-run performs a read-only preview and never writes data")
	fmt.Println("      missing lineage parent metadata is shown as Parent: (missing); snapshot data remains usable")
	fmt.Println("    Store codecs:")
	fmt.Println("      plain")
	fmt.Println("      aes-gcm")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  DB_HOST")
	fmt.Println("  DB_PORT")
	fmt.Println("  DB_USER")
	fmt.Println("  DB_PASSWORD")
	fmt.Println("  DB_NAME")
	fmt.Println("  DB_SSLMODE (default: disable)")
	fmt.Println("  COLDKEEP_DB_CONNECT_TIMEOUT_MS (default: 5000)")
	fmt.Println("  COLDKEEP_DB_OPERATION_TIMEOUT_MS (default: 300000)")
	fmt.Println("  COLDKEEP_DB_STATEMENT_TIMEOUT_MS (default: 30000)")
	fmt.Println("  COLDKEEP_DB_LOCK_TIMEOUT_MS (default: 5000)")
	fmt.Println("  COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS (default: 60000)")
	fmt.Println("  COLDKEEP_DB_MAX_OPEN_CONNS (default: 25)")
	fmt.Println("  COLDKEEP_DB_MAX_IDLE_CONNS (default: 5)")
	fmt.Println("  COLDKEEP_DB_CONN_MAX_LIFETIME_MS (default: 1800000)")
	fmt.Println("  COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS (default: 300000)")
	fmt.Println("  COLDKEEP_STORAGE_DIR (default: ./storage/containers)")
	fmt.Println("  COLDKEEP_CONTAINER_MAX_SIZE_MB (default: 64)")
	fmt.Println("  COLDKEEP_LOGICAL_FILE_WAIT_MS (default: 100)")
	fmt.Println("  COLDKEEP_CHUNK_WAIT_MS (default: 100)")
	fmt.Println("  COLDKEEP_MAX_CLAIM_POLL_WAIT_MS (default: 2000)")
	fmt.Println("  COLDKEEP_MAX_CLAIM_WAIT_MS (default: 120000)")
	fmt.Println("  COLDKEEP_CODEC (default: aes-gcm)")
	fmt.Println("  COLDKEEP_KEY (required for aes-gcm)")
	fmt.Println("  COLDKEEP_STRICT_RECOVERY (default: true; recommended for production)")
	fmt.Println("    true: fail startup on suspicious orphan container conflicts (intentional trust-first behavior)")
	fmt.Println("    false: warn and continue (relaxed mode for messy/retrier/restart-race environments)")
	fmt.Println("  COLDKEEP_REUSE_SEMANTIC_VALIDATION (default: suspicious)")
	fmt.Println("    off: graph-only reuse checks (fastest, no payload/hash re-validation)")
	fmt.Println("    suspicious: deep semantic checks only for risk signals (recommended)")
	fmt.Println("    always: deep semantic checks for every reuse candidate (highest read/CPU cost)")
	fmt.Println("  Startup recovery is corrective/state-changing and runs automatically before: store, store-folder, restore, remove, repair, gc, stats, list, search, verify, snapshot")
	fmt.Println("  Verify is observational and assumes recovered state (its verification phase is read-only)")
	fmt.Println("  Doctor runs its own corrective recovery pass even if startup recovery already ran")
	fmt.Println("  Batch JSON contract (restore/remove --output json): status=ok|partial_failure|error")
	fmt.Println("    ok: no item failed")
	fmt.Println("    partial_failure: at least one item failed and at least one item succeeded or was planned")
	fmt.Println("    error: all executable items failed")
	fmt.Println("  Batch process exit contract (restore/remove):")
	fmt.Println("    exit 0: no item failed")
	fmt.Println("    exit 1: any item failed (partial_failure or error)")
	fmt.Println("    exit 2: usage/validation error before execution")
	fmt.Println("  Simulated mode is not proof of physical durability")
	fmt.Println()
	fmt.Println("Operator quick check:")
	fmt.Println("  coldkeep doctor --standard")
	fmt.Println("  Recommended operator health gate: run coldkeep doctor after significant operations.")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  coldkeep init")
	fmt.Println("  coldkeep config get default-chunker")
	fmt.Println("  coldkeep config set default-chunker v2-fastcdc")
	fmt.Println("  coldkeep doctor --full")
	fmt.Println("  coldkeep store myfile.bin")
	fmt.Println("  coldkeep store --codec aes-gcm myfile.bin")
	fmt.Println("  coldkeep store-folder --codec plain ./samples")
	fmt.Println("  coldkeep list --limit 50 --offset 100")
	fmt.Println("  coldkeep search --name report --min-size 1024 --limit 25")
	fmt.Println("  coldkeep restore 12 ./restored")
	fmt.Println("  coldkeep remove 12")
	fmt.Println("  coldkeep repair ref-counts")
	fmt.Println("  coldkeep verify system --full")
	fmt.Println("  coldkeep verify file 12 --deep")
	fmt.Println("  coldkeep snapshot create")
	fmt.Println("  coldkeep snapshot create docs/ report.txt --label release-2026-04")
	fmt.Println("  coldkeep snapshot create --id day2 --from day1")
	fmt.Println("  coldkeep snapshot restore snap-123 --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 docs/ --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 docs/a.txt --mode override --destination ./restored/a.txt")
	fmt.Println("  coldkeep snapshot list --type full --limit 10")
	fmt.Println("  coldkeep snapshot list --tree")
	fmt.Println("  coldkeep snapshot show snap-123 --limit 50")
	fmt.Println("  coldkeep snapshot stats")
	fmt.Println("  coldkeep snapshot stats snap-123")
	fmt.Println("  coldkeep snapshot delete snap-123 --force")
	fmt.Println("  coldkeep snapshot delete snap-123 --dry-run")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --summary")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --filter modified")
	fmt.Println("  coldkeep snapshot show snap-123 --prefix docs/")
	fmt.Println("  coldkeep snapshot show snap-123 --pattern \"*.txt\" --min-size 1024")
	fmt.Println("  coldkeep snapshot restore snap-123 --prefix docs/ --mode prefix --destination ./restored")
	fmt.Println("  coldkeep snapshot restore snap-123 --pattern \"*.txt\" --overwrite --mode original")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --prefix docs/ --filter added")
	fmt.Println("  coldkeep snapshot diff snap-1 snap-2 --regex \"\\.log$\"")
	fmt.Println("  coldkeep gc --dry-run")
	fmt.Println("  coldkeep stats")
	fmt.Println("  coldkeep stats --json")
	fmt.Println("  coldkeep stats --containers")
	fmt.Println("  coldkeep simulate store myfile.bin")
	fmt.Println("  coldkeep simulate store-folder --codec aes-gcm ./samples")
}

func printHelpRows(rows [][2]string) {
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		if row[1] == "" {
			_, _ = fmt.Fprintln(writer, row[0])
			continue
		}
		_, _ = fmt.Fprintf(writer, "%s\t%s\n", row[0], row[1])
	}
	_ = writer.Flush()
}

func parseCommandLine(args []string, valueFlags map[string]bool) (parsedCommandLine, error) {
	if len(args) == 0 {
		return parsedCommandLine{}, usageErrorf("missing command")
	}

	parsed := parsedCommandLine{
		method:      args[0],
		positionals: make([]string, 0),
		flags:       make(map[string][]string),
	}

	for i := 1; i < len(args); i++ {
		arg := args[i]

		if arg == "--" {
			parsed.positionals = append(parsed.positionals, args[i+1:]...)
			break
		}

		if !strings.HasPrefix(arg, "--") {
			parsed.positionals = append(parsed.positionals, arg)
			continue
		}

		flagToken := strings.TrimPrefix(arg, "--")
		if name, value, found := strings.Cut(flagToken, "="); found {
			parsed.flags[name] = append(parsed.flags[name], value)
			continue
		}

		if valueFlags[flagToken] {
			if i+1 >= len(args) {
				return parsedCommandLine{}, usageErrorf("missing value for --%s", flagToken)
			}
			i++
			parsed.flags[flagToken] = append(parsed.flags[flagToken], args[i])
			continue
		}

		parsed.flags[flagToken] = append(parsed.flags[flagToken], "")
	}

	return parsed, nil
}

func (parsed parsedCommandLine) lastFlagValue(name string) (string, bool) {
	values, ok := parsed.flags[name]
	if !ok || len(values) == 0 {
		return "", false
	}

	return values[len(values)-1], true
}

func (parsed parsedCommandLine) flagValues(name string) []string {
	values, ok := parsed.flags[name]
	if !ok || len(values) == 0 {
		return nil
	}

	return append([]string(nil), values...)
}

func (parsed parsedCommandLine) hasFlag(names ...string) bool {
	for _, name := range names {
		if values, ok := parsed.flags[name]; ok && len(values) > 0 {
			return true
		}
	}

	return false
}

func ensureAllowedFlags(parsed parsedCommandLine, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, flag := range allowed {
		allowedSet[flag] = struct{}{}
	}

	var unknown []string
	for flag := range parsed.flags {
		if _, ok := allowedSet[flag]; !ok {
			unknown = append(unknown, flag)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	sort.Strings(unknown)
	return usageErrorf("unknown flag(s) for %s: %s", parsed.method, strings.Join(unknown, ", "))
}

func validateNonNegativeIntegerFlag(parsed parsedCommandLine, name string) error {
	value, ok := parsed.lastFlagValue(name)
	if !ok {
		return nil
	}

	parsedValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsedValue < 0 {
		return usageErrorf("invalid --%s value %q: must be a non-negative integer", name, value)
	}
	if name == "limit" && parsedValue > listing.MaxPaginationLimit {
		return usageErrorf("invalid --%s value %q: must be <= %d", name, value, listing.MaxPaginationLimit)
	}

	return nil
}

func listArgs(parsed parsedCommandLine) []string {
	args := make([]string, 0, 4)

	if value, ok := parsed.lastFlagValue("limit"); ok {
		args = append(args, "--limit", value)
	}
	if value, ok := parsed.lastFlagValue("offset"); ok {
		args = append(args, "--offset", value)
	}

	return args
}

func searchArgs(parsed parsedCommandLine) []string {
	orderedFlags := []string{"name", "min-size", "max-size"}
	args := make([]string, 0)

	for _, flag := range orderedFlags {
		for _, value := range parsed.flags[flag] {
			args = append(args, "--"+flag)
			if value != "" {
				args = append(args, value)
			}
		}
	}

	if value, ok := parsed.lastFlagValue("limit"); ok {
		args = append(args, "--limit", value)
	}
	if value, ok := parsed.lastFlagValue("offset"); ok {
		args = append(args, "--offset", value)
	}

	args = append(args, parsed.positionals...)
	return args
}

func parseVerifyLevel(parsed parsedCommandLine) (verify.VerifyLevel, error) {
	selected := make([]verify.VerifyLevel, 0, 1)
	if parsed.hasFlag("standard") {
		selected = append(selected, verify.VerifyStandard)
	}
	if parsed.hasFlag("full") {
		selected = append(selected, verify.VerifyFull)
	}
	if parsed.hasFlag("deep") {
		selected = append(selected, verify.VerifyDeep)
	}

	if len(selected) > 1 {
		return verify.VerifyStandard, usageErrorf("multiple verify levels provided")
	}

	positionalLevel := ""
	if len(parsed.positionals) == 2 && parsed.positionals[0] == "system" {
		positionalLevel = parsed.positionals[1]
	}
	if len(parsed.positionals) == 3 && parsed.positionals[0] == "file" {
		positionalLevel = parsed.positionals[2]
	}

	if positionalLevel != "" {
		if len(selected) > 0 {
			return verify.VerifyStandard, usageErrorf("verify level provided both as flag and positional argument")
		}

		switch positionalLevel {
		case "standard":
			return verify.VerifyStandard, nil
		case "full":
			return verify.VerifyFull, nil
		case "deep":
			return verify.VerifyDeep, nil
		default:
			return verify.VerifyStandard, usageErrorf("unknown verify level: %s", positionalLevel)
		}
	}

	if len(selected) == 1 {
		return selected[0], nil
	}

	return verify.VerifyStandard, nil
}
