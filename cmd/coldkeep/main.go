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
	"github.com/franchoy/coldkeep/internal/verify"
)

const version = "0.6.0"

func main() {

	//system recovery on startup
	err := recovery.SystemRecovery()
	if err != nil {
		log.Printf("System recovery failed: %v\n", err)
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
		fileID, parseErr := strconv.ParseInt(os.Args[2], 10, 64)
		if parseErr != nil {
			log.Fatal("Invalid fileID: ", parseErr)
		}

		err = storage.RestoreFile(fileID, os.Args[3])

	case "remove":
		if len(os.Args) < 3 {
			log.Fatal("Usage: coldkeep remove <fileID>")
		}
		fileID, parseErr := strconv.ParseInt(os.Args[2], 10, 64)
		if parseErr != nil {
			log.Fatal("Invalid fileID: ", parseErr)
		}

		err = storage.RemoveFile(fileID)

	case "gc":
		if len(os.Args) > 2 {
			switch os.Args[2] {
			case "--dry-run", "--dryRun", "dry-run", "dryRun":
				err = maintenance.RunGC(true)
			default:
				log.Fatal("Unknown option for gc: ", os.Args[2])
			}
		} else {
			err = maintenance.RunGC(false)
		}

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

	case "verify":
		var target string
		var verifyLevel verify.VerifyLevel
		var fileID int64
		//target can be "system" or "file"
		if len(os.Args) > 2 {
			target = os.Args[2]
			switch target {
			case "system":
				//target is system, verify level can be --standard, --full, or --deep
				if len(os.Args) > 3 {
					switch os.Args[3] {
					case "--standard", "standard", "":
						verifyLevel = verify.VerifyStandard
					case "--full", "full":
						verifyLevel = verify.VerifyFull
					case "--deep", "deep":
						verifyLevel = verify.VerifyDeep
					default:
						log.Fatal("Unknown option for system verify: ", os.Args[3])
					}
				} else {
					verifyLevel = verify.VerifyStandard
				}
			case "file":
				if len(os.Args) > 3 {
					fileID, err = strconv.ParseInt(os.Args[3], 10, 64)
					if err != nil {
						log.Fatal("Invalid fileID: ", err)
					}
				} else {
					log.Fatal("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
				}
				if len(os.Args) > 4 {
					switch os.Args[4] {
					case "--standard", "standard", "":
						verifyLevel = verify.VerifyStandard
					case "--full", "full":
						verifyLevel = verify.VerifyFull
					case "--deep", "deep":
						verifyLevel = verify.VerifyDeep
					default:
						log.Fatal("Unknown option for file verify: ", os.Args[4])
					}
				} else {
					verifyLevel = verify.VerifyStandard
				}
			default:
				log.Fatal("Unknown target for verify: ", target)
			}
			//call verify command with target, fileID, and verifyLevel
			err = maintenance.VerifyCommand(target, int(fileID), verifyLevel)
		} else {
			log.Fatal("Usage: coldkeep verify file <fileID> [--standard|--full|--deep]")
		}

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
	fmt.Println("coldkeep (V0.6.0)")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  coldkeep <command> [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  store <file>               			Store a single file")
	fmt.Println("  store-folder <folder>     			Store all files in a folder recursively")
	fmt.Println("  restore <fileID> <dir>     			Restore file by ID into directory")
	fmt.Println("  remove <fileID>            			Remove logical file (decrement refcounts)")
	fmt.Println("  gc [options]               			Run garbage collection")
	fmt.Println("    (no options)             				Perform standard GC")
	fmt.Println("  gc --dry-run               				Show what would be removed without deleting")
	fmt.Println("  stats                      			Show storage statistics")
	fmt.Println("  verify [target] [fileID] [options]	Verify stored files")
	fmt.Println("  		  [target] can be 'system' or 'file'")
	fmt.Println("  		  					[options] can be '--standard', '--full', or '--deep'")
	fmt.Println("  		  						no options defaults to '--standard'")
	fmt.Println("    	verify system [options]         	Perform system-wide verification")
	fmt.Println("    	verify file <fileID> [options]  	Perform verification for specific file")
	fmt.Println("  help                       			Show this help message")
	fmt.Println("  version                    			Show version information")
	fmt.Println("  list                      			List stored logical files")
	fmt.Println("  search [filters]          			Search files by filters")
	fmt.Println("			Filters:")
	fmt.Println("  				--name <substring>")
	fmt.Println("  				--min-size <bytes>")
	fmt.Println("  				--max-size <bytes>")
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
