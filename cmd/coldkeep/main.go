package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/franchoy/coldkeep/internal/listing"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
)

const version = "0.1.0"

func main() {

	//system recovery on startup
	err := recovery.SystemRecovery()
	if err != nil {
		log.Printf("System recovery failed: %v\n", err)
		os.Exit(1)
	}

	if len(os.Args) < 2 {
		printHelp()
		return
	}

	switch os.Args[1] {
	case "store":
		if len(os.Args) < 3 {
			log.Fatal("Usage: coldkeep store <filePath>")
		}
		err = storage.StoreFile(os.Args[2])

	case "store-folder":
		if len(os.Args) < 3 {
			log.Fatal("Usage: coldkeep store-folder <folderPath>")
		}
		err = storage.StoreFolder(os.Args[2])

	case "restore":
		if len(os.Args) < 4 {
			log.Fatal("Usage: coldkeep restore <fileID> <outputDir>")
		}
		fileID, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			log.Fatal("Invalid fileID: ", err)
		}
		err = storage.RestoreFile(fileID, os.Args[3])

	case "remove":
		if len(os.Args) < 3 {
			log.Fatal("Usage: coldkeep remove <fileID>")
		}
		fileID, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			log.Fatal("Invalid fileID: ", err)
		}
		err = storage.RemoveFile(fileID)

	case "gc":
		err = maintenance.RunGC()

	case "stats":
		err = maintenance.RunStats()

	case "help", "-h", "--help":
		printHelp()

	case "version", "-v", "--version":
		fmt.Println("coldkeep version", version)

	case "list":
		err = listing.ListFiles()

	case "search":
		err = listing.SearchFiles(os.Args[2:])

	default:
		fmt.Println("Unknown command:", os.Args[1])
		fmt.Println()
		printHelp()

	}

	if err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}

}

func printHelp() {
	fmt.Println("coldkeep POC (V0)")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  store <file>               Store a single file")
	fmt.Println("  store-folder <folder>      Store all files in a folder recursively")
	fmt.Println("  restore <fileID> <dir>     Restore file by ID into directory")
	fmt.Println("  remove <fileID>            Remove logical file (decrement refcounts)")
	fmt.Println("  gc                         Run garbage collection")
	fmt.Println("  stats                      Show storage statistics")
	fmt.Println("  help                       Show this help message")
	fmt.Println("  version                    Show version information")
	fmt.Println("  list                       List stored logical files")
	fmt.Println("  search [filters]           Search files by filters")
	fmt.Println()
	fmt.Println("Search Filters:")
	fmt.Println("  --name <substring>")
	fmt.Println("  --min-size <bytes>")
	fmt.Println("  --max-size <bytes>")
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
	fmt.Println("  coldkeep restore 12 ./restored")
}
