package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const version = "0.1.0"

var defaultCompression = CompressionZstd //CompressionNone

func main() {
	if len(os.Args) < 2 {
		printHelp()
		return
	}

	var err error

	switch os.Args[1] {
	case "store":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule store <filePath>")
		}
		err = storeFile(os.Args[2])

	case "store-folder":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule store-folder <folderPath>")
		}
		err = storeFolder(os.Args[2])

	case "restore":
		if len(os.Args) < 4 {
			log.Fatal("Usage: capsule restore <fileID> <outputDir>")
		}
		fileID, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			log.Fatal("Invalid fileID: ", err)
		}
		err = restoreFile(fileID, os.Args[3])

	case "remove":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule remove <fileID>")
		}
		err = removeFile(os.Args[2])

	case "gc":
		err = runGC()

	case "stats":
		err = runStats()

	case "help", "-h", "--help":
		printHelp()

	case "version", "-v", "--version":
		fmt.Println("Capsule version", version)

	case "list":
		err = listFiles()

	case "search":
		err = searchFiles(os.Args[2:])

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
	fmt.Println("Capsule POC (V0)")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  capsule <command> [arguments]")
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
	fmt.Println("  CAPSULE_STORAGE_DIR (default: ./storage/containers)")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  capsule store myfile.bin")
	fmt.Println("  capsule restore 12 ./restored")
}

func printSuccess(title string) {
	fmt.Println(title)
}

func printDuration(start time.Time) {
	fmt.Printf("  Time: %v\n", time.Since(start))
}
