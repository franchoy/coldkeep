package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

const version = "0.6.0"

var flagsWithValues = map[string]bool{
	"codec":    true,
	"name":     true,
	"min-size": true,
	"max-size": true,
}

type parsedCommandLine struct {
	method      string
	positionals []string
	flags       map[string][]string
}

func main() {
	err := recovery.SystemRecovery()
	if err != nil {
		log.Printf("System recovery failed: %v\n", err)
	}

	if len(os.Args) < 2 {
		printHelp()
		return
	}

	parsed, err := parseCommandLine(os.Args[1:], flagsWithValues)
	if err != nil {
		log.Fatal(err)
	}

	switch parsed.method {
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
		err = runStatsCommand(parsed)
	case "help", "-h", "--help":
		printHelp()
	case "version", "-v", "--version":
		fmt.Println("coldkeep version", version)
	case "list":
		err = runListCommand(parsed)
	case "search":
		err = runSearchCommand(parsed)
	case "verify":
		err = runVerifyCommand(parsed)
	default:
		fmt.Println("Unknown command:", parsed.method)
		fmt.Println()
		printHelp()
	}

	if err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
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
	if codecName == "" {
		return storage.StoreFile(path)
	}

	return storage.StoreFileWithCodec(path, codecName)
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
	if codecName == "" {
		return storage.StoreFolder(path)
	}

	return storage.StoreFolderWithCodec(path, codecName)
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

	return storage.RestoreFile(fileID, parsed.positionals[1])
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

	return maintenance.RunGC(dryRun)
}

func runStatsCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return errors.New("Usage: coldkeep stats")
	}

	return maintenance.RunStats()
}

func runListCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed); err != nil {
		return err
	}
	if len(parsed.positionals) != 0 {
		return errors.New("Usage: coldkeep list")
	}

	return listing.ListFiles()
}

func runSearchCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "name", "min-size", "max-size"); err != nil {
		return err
	}

	return listing.SearchFiles(searchArgs(parsed))
}

func runVerifyCommand(parsed parsedCommandLine) error {
	if err := ensureAllowedFlags(parsed, "standard", "full", "deep"); err != nil {
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
		return maintenance.VerifyCommand(target, 0, verifyLevel)
	case "file":
		if len(parsed.positionals) < 2 || len(parsed.positionals) > 3 {
			return errors.New("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
		}

		fileID, err := strconv.ParseInt(parsed.positionals[1], 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid fileID: %w", err)
		}

		return maintenance.VerifyCommand(target, int(fileID), verifyLevel)
	default:
		return fmt.Errorf("Unknown target for verify: %s", target)
	}
}

func printHelp() {
	fmt.Println("coldkeep (V0.6.0)")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	printHelpRows([][2]string{
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
	fmt.Println()
	fmt.Println("Example:")
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
