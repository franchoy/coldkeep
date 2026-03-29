package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

const version = "0.7.0"

const (
	exitSuccess = 0
	exitGeneral = 1
	exitUsage   = 2
	exitVerify  = 3
)

var flagsWithValues = map[string]bool{
	"codec":    true,
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

	err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir)
	if err != nil {
		log.Printf("System recovery failed: %v\n", err)
	}

	if startupMode != outputModeJSON {
		checkEnvFilePermissions()
	}

	if len(args) < 1 {
		printHelp()
		return exitSuccess
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
	case "store":
		err = runStoreCommand(parsed)
	case "store-folder":
		err = runStoreFolderCommand(parsed)
	case "restore":
		err = runRestoreCommand(parsed)
	case "remove":
		err = runRemoveCommand(parsed)
	case "gc":
		err = runGCCommand(parsed)
	case "stats":
		err = runStatsCommand(parsed, outputMode)
	case "help", "-h", "--help":
		printHelp()
	case "version", "-v", "--version":
		fmt.Println("coldkeep version", version)
	case "list":
		err = runListCommand(parsed, outputMode)
	case "search":
		err = runSearchCommand(parsed, outputMode)
	case "verify":
		err = runVerifyCommand(parsed)
	default:
		err = fmt.Errorf("unknown command: %s", parsed.method)
	}

	if err != nil {
		return printCLIError(err, outputMode)
	}

	printCLISuccess(parsed, outputMode)

	return exitSuccess
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
	if parsed.method != "verify" {
		// list/search/stats emit their own JSON payload; nothing to add here.
		return
	}

	payload := map[string]any{
		"status":  "ok",
		"command": parsed.method,
	}

	if len(parsed.positionals) > 0 {
		payload["target"] = parsed.positionals[0]
	}
	if verifyLevel, err := parseVerifyLevel(parsed); err == nil {
		payload["level"] = verifyLevelToString(verifyLevel)
	}

	encoded, _ := json.Marshal(payload)
	fmt.Println(string(encoded))
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
	value, hasValue := parsed.firstFlagValue("output")
	if !hasValue {
		return outputModeText, nil
	}

	switch parsed.method {
	case "verify", "list", "search", "stats":
		// --output supported
	default:
		return outputModeText, fmt.Errorf("--output is not supported for command %q", parsed.method)
	}

	switch strings.ToLower(value) {
	case "", "text":
		return outputModeText, nil
	case "json":
		return outputModeJSON, nil
	default:
		return outputModeText, fmt.Errorf("invalid --output value %q (allowed: text, json)", value)
	}
}

var outputSupportedCommands = map[string]bool{
	"verify": true,
	"list":   true,
	"search": true,
	"stats":  true,
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
		strings.Contains(msg, "verify level provided both as flag and positional argument") {
		return exitUsage
	}

	if strings.Contains(msg, "verification failed") || strings.Contains(msg, "verify ") {
		return exitVerify
	}

	return exitGeneral
}

func runStoreCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "codec"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return errors.New("Usage: coldkeep store [--codec <plain|aes-gcm>] <filePath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.firstFlagValue("codec")

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if codecName == "" {
		return storage.StoreFileWithStorageContext(sgctx, path)
	}

	if codecName == "plain" {
		fmt.Fprintln(os.Stderr, "WARNING: storing data without encryption")
	}

	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	return storage.StoreFileWithStorageContextAndCodec(sgctx, path, codec)
}

func runStoreFolderCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "codec"); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return errors.New("Usage: coldkeep store-folder [--codec <plain|aes-gcm>] <folderPath>")
	}

	path := parsed.positionals[0]
	codecName, _ := parsed.firstFlagValue("codec")

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	if codecName == "" {
		return storage.StoreFolderWithStorageContext(sgctx, path)
	}

	if codecName == "plain" {
		fmt.Fprintln(os.Stderr, "WARNING: storing data without encryption")
	}

	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	return storage.StoreFolderWithStorageContextAndCodec(sgctx, path, codec)
}

func runRestoreCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed); err != nil {
		return err
	}
	if len(parsed.positionals) != 2 {
		return errors.New("Usage: coldkeep restore <fileID> <outputDir>")
	}

	fileID, err := strconv.ParseInt(parsed.positionals[0], 10, 64)
	if err != nil {
		return fmt.Errorf("Invalid fileID: %w", err)
	}

	sgctx, err := storage.LoadDefaultStorageContext()
	if err != nil {
		return fmt.Errorf("load storage context: %w", err)
	}
	defer func() { _ = sgctx.Close() }()

	return storage.RestoreFileWithStorageContext(sgctx, fileID, parsed.positionals[1])
}

func runRemoveCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed); err != nil {
		return err
	}
	if len(parsed.positionals) != 1 {
		return errors.New("Usage: coldkeep remove <fileID>")
	}

	fileID, err := strconv.ParseInt(parsed.positionals[0], 10, 64)
	if err != nil {
		return fmt.Errorf("Invalid fileID: %w", err)
	}

	return storage.RemoveFile(fileID)
}

func runGCCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "dry-run", "dryRun"); err != nil {
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
			return fmt.Errorf("Unknown option for gc: %s", parsed.positionals[0])
		}
	default:
		return errors.New("Usage: coldkeep gc [--dry-run]")
	}

	return maintenance.RunGCWithContainersDir(dryRun, container.ContainersDir)
}

func runStatsCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return errors.New("Usage: coldkeep stats")
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

	return maintenance.RunStats()
}

func runListCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "output"); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return errors.New("Usage: coldkeep list")
	}

	if outputMode == outputModeJSON {
		files, err := listing.ListFilesResult()
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

	return listing.ListFiles()
}

func runSearchCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	if err := ensureAllowedFlags(parsed, "name", "min-size", "max-size", "output"); err != nil {
		return err
	}

	if outputMode == outputModeJSON {
		files, err := listing.SearchFilesResult(searchArgs(parsed))
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

	return listing.SearchFiles(searchArgs(parsed))
}

func runVerifyCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep", "output"); err != nil {
		return err
	}
	if len(parsed.positionals) == 0 {
		return errors.New("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
	}

	verifyLevel, err := parseVerifyLevel(parsed)
	if err != nil {
		return err
	}

	target := parsed.positionals[0]
	switch target {
	case "system":
		if len(parsed.positionals) > 2 {
			return errors.New("Usage: coldkeep verify system [--standard|--full|--deep]")
		}
		return maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, 0, verifyLevel)
	case "file":
		if len(parsed.positionals) < 2 || len(parsed.positionals) > 3 {
			return errors.New("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
		}

		fileID, err := strconv.ParseInt(parsed.positionals[1], 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid fileID: %w", err)
		}

		return maintenance.VerifyCommandWithContainersDir(container.ContainersDir, target, int(fileID), verifyLevel)
	default:
		return fmt.Errorf("Unknown target for verify: %s", target)
	}
}

func printHelp() {
	fmt.Println("coldkeep (V0.7.0)")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	printHelpRows([][2]string{
		{"  init", "Initialize Coldkeep with a new aes-gcm encryption key"},
		{"  store [--codec <codec>] <file>", "Store a single file"},
		{"  store-folder [--codec <codec>] <folder>", "Store all files in a folder recursively"},
		{"  restore <fileID> <dir>", "Restore file by ID into directory"},
		{"  remove <fileID>", "Remove logical file (decrement refcounts)"},
		{"  gc [options]", "Run garbage collection"},
		{"    (no options)", "Perform standard GC"},
		{"    --dry-run", "Show what would be removed without deleting"},
		{"  stats", "Show storage statistics"},
		{"  verify [target] [fileID] [options]", "Verify stored files"},
		{"    [target] can be 'system' or 'file'", ""},
		{"    [options] can be '--standard', '--full', or '--deep'", ""},
		{"    no options defaults to '--standard'", ""},
		{"    verify system [options]", "Perform system-wide verification"},
		{"    verify file <fileID> [options]", "Perform verification for specific file"},
		{"  help", "Show this help message"},
		{"  version", "Show version information"},
		{"  list", "List stored logical files"},
		{"  search [filters]", "Search files by filters"},
	})
	fmt.Println("    Filters:")
	fmt.Println("      --name <substring>")
	fmt.Println("      --min-size <bytes>")
	fmt.Println("      --max-size <bytes>")
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
	fmt.Println("  COLDKEEP_STORAGE_DIR (default: ./storage/containers)")
	fmt.Println("  COLDKEEP_CONTAINER_MAX_SIZE_MB (default: 64)")
	fmt.Println("  COLDKEEP_CODEC (default: aes-gcm)")
	fmt.Println("  COLDKEEP_KEY (required for aes-gcm)")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  coldkeep init")
	fmt.Println("  coldkeep store myfile.bin")
	fmt.Println("  coldkeep store --codec aes-gcm myfile.bin")
	fmt.Println("  coldkeep store-folder --codec plain ./samples")
	fmt.Println("  coldkeep list")
	fmt.Println("  coldkeep search --name report --min-size 1024")
	fmt.Println("  coldkeep restore 12 ./restored")
	fmt.Println("  coldkeep remove 12")
	fmt.Println("  coldkeep verify system --full")
	fmt.Println("  coldkeep verify file 12 --deep")
	fmt.Println("  coldkeep gc --dry-run")
	fmt.Println("  coldkeep stats")
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
		return parsedCommandLine{}, errors.New("missing command")
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
				return parsedCommandLine{}, fmt.Errorf("missing value for --%s", flagToken)
			}
			i++
			parsed.flags[flagToken] = append(parsed.flags[flagToken], args[i])
			continue
		}

		parsed.flags[flagToken] = append(parsed.flags[flagToken], "")
	}

	return parsed, nil
}

func (parsed parsedCommandLine) firstFlagValue(name string) (string, bool) {
	values, ok := parsed.flags[name]
	if !ok || len(values) == 0 {
		return "", false
	}

	return values[0], true
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
	return fmt.Errorf("unknown flag(s) for %s: %s", parsed.method, strings.Join(unknown, ", "))
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
		return verify.VerifyStandard, errors.New("multiple verify levels provided")
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
			return verify.VerifyStandard, errors.New("verify level provided both as flag and positional argument")
		}

		switch positionalLevel {
		case "standard":
			return verify.VerifyStandard, nil
		case "full":
			return verify.VerifyFull, nil
		case "deep":
			return verify.VerifyDeep, nil
		default:
			return verify.VerifyStandard, fmt.Errorf("unknown verify level: %s", positionalLevel)
		}
	}

	if len(selected) == 1 {
		return selected[0], nil
	}

	return verify.VerifyStandard, nil
}
