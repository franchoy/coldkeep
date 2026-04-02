package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/internal/version"
)

const (
	exitSuccess = 0
	exitGeneral = 1
	exitUsage   = 2
	exitVerify  = 3
)

var stdoutRedirectMu sync.Mutex

var flagsWithValues = map[string]bool{
	"codec":    true,
	"limit":    true,
	"offset":   true,
	"name":     true,
	"min-size": true,
	"max-size": true,
	"output":   true,
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

type doctorReport struct {
	Recovery       recovery.Report `json:"recovery"`
	VerifyLevel    string          `json:"verify_level"`
	SchemaVersion  int64           `json:"schema_version"`
	RecoveryStatus string          `json:"recovery_status"`
	VerifyStatus   string          `json:"verify_status"`
	SchemaStatus   string          `json:"schema_status"`
}

const doctorDefaultVerifyLevel = verify.VerifyStandard

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
		recoveryReport, recoveryErr := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
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
		return printCLIError(err, outputModeText)
	}

	outputMode, err := resolveOutputMode(parsed)
	if err != nil {
		return printCLIError(err, outputModeText)
	}

	switch parsed.method {
	case "init":
		err = initCommand()
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
	case "gc":
		err = runGCCommand(parsed, outputMode)
	case "simulate":
		err = runSimulateCommand(parsed, outputMode)
	case "stats":
		err = runStatsCommand(parsed, outputMode)
	case "help", "-h", "--help":
		printHelp()
	case "version", "-v", "--version":
		err = runVersionCommand(outputMode)
	case "list":
		err = runListCommand(parsed, outputMode)
	case "search":
		err = runSearchCommand(parsed, outputMode)
	case "verify":
		err = runVerifyCommand(parsed)
	default:
		err = usageErrorf("unknown command: %s", parsed.method)
	}

	if err != nil {
		return printCLIError(err, outputMode)
	}

	printCLISuccess(parsed, outputMode)

	return exitSuccess
}

func emitStartupRecoveryReport(mode cliOutputMode, report recovery.Report, err error) {
	if mode == outputModeJSON {
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
	if mode == outputModeJSON {
		payload := map[string]any{
			"status":      "error",
			"error_class": exitCodeLabel(code),
			"exit_code":   code,
			"message":     strings.TrimSpace(err.Error()),
		}
		encoded, _ := json.Marshal(payload)
		fmt.Fprintln(os.Stderr, string(encoded))
		return code
	}

	fmt.Fprintf(os.Stderr, "ERROR[%s]: %s\n", exitCodeLabel(code), strings.TrimSpace(err.Error()))
	return code
}

func printCLISuccess(parsed parsedCommandLine, mode cliOutputMode) {
	if mode != outputModeJSON {
		return
	}
	// These commands emit their own structured JSON payload.
	switch parsed.method {
	case "store", "store-folder", "restore", "remove", "gc", "list", "search", "stats", "simulate", "doctor", "version", "-v", "--version":
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
	if !hasValue {
		return outputModeText, nil
	}

	switch strings.ToLower(value) {
	case "", "text":
		return outputModeText, nil
	case "json":
		return outputModeJSON, nil
	default:
		return outputModeText, usageErrorf("invalid --output value %q (allowed: text, json)", value)
	}
}

var outputSupportedCommands = map[string]bool{
	"doctor":       true,
	"verify":       true,
	"list":         true,
	"search":       true,
	"stats":        true,
	"store":        true,
	"store-folder": true,
	"restore":      true,
	"remove":       true,
	"gc":           true,
	"simulate":     true,
}

func inferOutputModeFromArgs(args []string) cliOutputMode {
	if len(args) < 1 || !outputSupportedCommands[args[0]] {
		return outputModeText
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
	// doctor runs its own recovery phase inside runDoctorCommand so it can
	// report recovery/verify/schema in a single command-specific payload.
	case "store", "store-folder", "restore", "remove", "gc", "stats", "list", "search", "verify":
		return true
	default:
		return false
	}
}

func exitCodeLabel(code int) string {
	switch code {
	case exitUsage:
		return "USAGE"
	case exitVerify:
		return "VERIFY"
	default:
		return "GENERAL"
	}
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
		strings.Contains(msg, "invalid fileid") ||
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

	if strings.Contains(msg, "verification failed") || strings.Contains(msg, "verify ") {
		return exitVerify
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

	sgctx, err := storage.LoadDefaultStorageContext()
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
	return nil
}

func runRestoreCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 2 {
		return usageErrorf("Usage: coldkeep restore <fileID> <outputDir>")
	}

	fileID, err := strconv.ParseInt(parsed.positionals[0], 10, 64)
	if err != nil {
		return usageErrorf("Invalid fileID: %v", err)
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	outPath := parsed.positionals[1]
	result, err := storage.RestoreFileWithStorageContextResult(sgctx, fileID, outPath)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "restore",
			"data":    result,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Printf("File restored successfully: id=%d output=%s\n", result.FileID, result.OutputPath)
	fmt.Printf("  Name: %s\n", result.OriginalName)
	fmt.Printf("  SHA256: %s\n", result.RestoredHash)
	return nil
}

func runRemoveCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return usageErrorf("Usage: coldkeep remove <fileID>")
	}

	fileID, err := strconv.ParseInt(parsed.positionals[0], 10, 64)
	if err != nil {
		return usageErrorf("Invalid fileID: %v", err)
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	result, err := storage.RemoveFileWithDBResult(sgctx.DB, fileID)
	if err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		payload := map[string]any{
			"status":  "ok",
			"command": "remove",
			"data":    result,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Printf("Logical file removed: id=%d\n", result.FileID)
	fmt.Printf("  Removed mappings: %d\n", result.RemovedMappings)
	return nil
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

	result, err := maintenance.RunGCWithContainersDirResult(dryRun, container.ContainersDir)
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
		return nil
	}

	if dryRun {
		for _, filename := range result.ContainerFilenames {
			fmt.Printf("[DRY-RUN] Would delete container: %s\n", filename)
		}
		fmt.Printf("GC dry-run completed. Containers eligible for deletion: %d\n", result.AffectedContainers)
		return nil
	}

	for _, filename := range result.ContainerFilenames {
		fmt.Printf("Deleted container: %s\n", filename)
	}
	fmt.Printf("GC completed. Containers deleted: %d\n", result.AffectedContainers)
	return nil
}

func runStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return usageErrorf("Usage: coldkeep stats")
	}

	if outputMode == outputModeJSON {
		r, err := maintenance.RunStatsResult()
		if err != nil {
			return err
		}
		payload := map[string]any{
			"status":  "ok",
			"command": "stats",
			"data":    r,
		}
		encoded, _ := json.Marshal(payload)
		fmt.Println(string(encoded))
		return nil
	}

	r, err := maintenance.RunStatsResult()
	if err != nil {
		return err
	}
	printStatsReport(r)
	return nil
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
	fmt.Printf("%-6s %-25s %-15s %-20s\n", "ID", "NAME", "SIZE(bytes)", "CREATED_AT")
	fmt.Println("---------------------------------------------------------------------")
	for _, r := range records {
		fmt.Printf("%-6d %-25s %-15d %-20s\n", r.ID, r.Name, r.SizeBytes, r.CreatedAt)
	}
}

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func printStatsReport(r *maintenance.StatsResult) {
	fmt.Println("\n====== coldkeep Stats ======")
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
	fmt.Printf("File retry stats:                total=%d, avg=%.2f, max=%d\n", r.TotalFileRetries, r.AvgFileRetries, r.MaxFileRetries)
	fmt.Printf("Chunk retry stats:               total=%d, avg=%.2f, max=%d\n", r.TotalChunkRetries, r.AvgChunkRetries, r.MaxChunkRetries)
	fmt.Println("============================")
	fmt.Printf("Chunks (total):           %d\n", r.TotalChunks)
	fmt.Printf("  Completed chunks:       %d (%.2f MB)\n", r.CompletedChunks, bytesToMB(r.CompletedChunkBytes))
	fmt.Printf("  Processing chunks:      %d\n", r.ProcessingChunks)
	fmt.Printf("  Aborted chunks:         %d\n", r.AbortedChunks)
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

// runVerifyCommand executes recovered-state verification. It is not intended
// to be an online checker during active writes, where transient metadata/data
// divergence can produce false positives.
func runVerifyCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) == 0 {
		return usageErrorf("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
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
		return verifyError(maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, 0, verifyLevel))
	case "file":
		if len(parsed.positionals) < 2 || len(parsed.positionals) > 3 {
			return usageErrorf("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
		}

		fileID, err := strconv.ParseInt(parsed.positionals[1], 10, 64)
		if err != nil {
			return usageErrorf("Invalid fileID: %v", err)
		}

		return verifyError(maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, int(fileID), verifyLevel))
	default:
		return usageErrorf("Unknown target for verify: %s", target)
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

	recoveryReport, recoveryErr := recovery.SystemRecoveryReportWithContainersDir(container.ContainersDir)
	report.Recovery = recoveryReport
	if recoveryErr != nil {
		report.RecoveryStatus = "error"
	} else {
		report.RecoveryStatus = "ok"
	}

	schemaVersion, schemaErr := querySchemaVersion()
	if schemaErr != nil {
		report.SchemaStatus = "error"
	} else {
		report.SchemaVersion = schemaVersion
		report.SchemaStatus = "ok"
	}

	verifyErr := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verifyLevel)
	if verifyErr != nil {
		report.VerifyStatus = "error"
	} else {
		report.VerifyStatus = "ok"
	}

	// Intentional JSON contract:
	// - Success: doctor-specific payload on stdout.
	// - Failure: generic CLI error payload on stderr via printCLIError.
	// Doctor does not emit partial doctor data on failure.
	if recoveryErr != nil {
		return fmt.Errorf("doctor recovery phase failed: %w", recoveryErr)
	}
	if schemaErr != nil {
		return fmt.Errorf("doctor schema/version check failed: %w", schemaErr)
	}
	if verifyErr != nil {
		return verifyError(fmt.Errorf("doctor verify phase failed: %w", verifyErr))
	}

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
		overallStatus := "ok"
		if report.RecoveryStatus != "ok" || report.VerifyStatus != "ok" || report.SchemaStatus != "ok" {
			overallStatus = "error"
		}

		fmt.Println("Doctor health report")
		fmt.Printf("  Overall status:      %s\n", overallStatus)
		fmt.Printf("  Verify level:        %s\n", report.VerifyLevel)
		fmt.Printf("  Phase 1 - Recovery:  %s\n", report.RecoveryStatus)
		fmt.Printf("  Phase 2 - Verify:    %s\n", report.VerifyStatus)
		if report.SchemaStatus == "ok" {
			fmt.Printf("  Phase 3 - Schema:    %s (version=%d)\n", report.SchemaStatus, report.SchemaVersion)
		} else {
			fmt.Printf("  Phase 3 - Schema:    %s\n", report.SchemaStatus)
		}
		fmt.Printf("  Recovery summary: aborted_logical_files=%d aborted_chunks=%d quarantined_missing_containers=%d quarantined_corrupt_tail_containers=%d quarantined_orphan_containers=%d\n",
			report.Recovery.AbortedLogicalFiles,
			report.Recovery.AbortedChunks,
			report.Recovery.QuarantinedMissing,
			report.Recovery.QuarantinedCorruptTail,
			report.Recovery.QuarantinedOrphan,
		)
	}

	return nil
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

func printHelp() {
	fmt.Printf("coldkeep (v%s)\n", version.String())
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	printHelpRows([][2]string{
		{"  init", "Initialize Coldkeep with a new aes-gcm encryption key"},
		{"  doctor [--standard|--full|--deep] [--output <text|json>]", "Corrective health command (may update metadata via recovery before verify; default: --standard)"},
		{"  store [--codec <codec>] <file>", "Store a single file"},
		{"  store-folder [--codec <codec>] <folder>", "Store all files in a folder recursively"},
		{"  restore <fileID> <dir>", "Restore file by ID into directory (accepts COMPLETED chunks from any container, sealed or active)"},
		{"  remove <fileID>", "Remove logical file (decrements chunk reference counts)"},
		{"  gc [options]", "Run garbage collection"},
		{"    (no options)", "Remove unreferenced data"},
		{"    --dry-run", "Show what would be removed without deleting"},
		{"  stats", "Show storage statistics"},
		{"  verify [target] [fileID] [options]", "Layered integrity verification (default: --standard)"},
		{"    [target] can be 'system' or 'file'", ""},
		{"    [options] can be '--standard', '--full', or '--deep'", ""},
		{"    no options defaults to '--standard'", ""},
		{"    verify system [options]", "Perform system-wide verification"},
		{"    verify file <fileID> [options]", "Perform verification for specific file"},
		{"  help", "Show this help message"},
		{"  version", "Show version information"},
		{"  list [--limit <count>] [--offset <count>]", "List stored logical files"},
		{"  search [filters] [--limit <count>] [--offset <count>]", "Search files by filters"},
		{"  simulate <store|store-folder> <path>", "Dry-run store without writing to storage"},
	})
	fmt.Println("    Filters:")
	fmt.Println("      --name <substring>")
	fmt.Println("      --min-size <bytes>")
	fmt.Println("      --max-size <bytes>")
	fmt.Println("      --limit <count>")
	fmt.Println("      --offset <count>")
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
	fmt.Println("  Startup recovery runs automatically before: store, store-folder, restore, remove, gc, stats, list, search, verify")
	fmt.Println()
	fmt.Println("Operator quick check:")
	fmt.Println("  coldkeep doctor --standard")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  coldkeep init")
	fmt.Println("  coldkeep doctor --full")
	fmt.Println("  coldkeep store myfile.bin")
	fmt.Println("  coldkeep store --codec aes-gcm myfile.bin")
	fmt.Println("  coldkeep store-folder --codec plain ./samples")
	fmt.Println("  coldkeep list --limit 50 --offset 100")
	fmt.Println("  coldkeep search --name report --min-size 1024 --limit 25")
	fmt.Println("  coldkeep restore 12 ./restored")
	fmt.Println("  coldkeep remove 12")
	fmt.Println("  coldkeep verify system --full")
	fmt.Println("  coldkeep verify file 12 --deep")
	fmt.Println("  coldkeep gc --dry-run")
	fmt.Println("  coldkeep stats")
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
