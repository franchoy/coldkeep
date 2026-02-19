package main

import (
	"log"
	"os"
	"strconv"
	"sync"
)

var defaultCompression = CompressionZstd // change if needed
var containerMutex sync.Mutex

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: capsule <store|store-folder|restore|remove|gc|stats> ...")
	}

	switch os.Args[1] {
	case "store":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule store <filePath>")
		}
		storeFile(os.Args[2])

	case "store-folder":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule store-folder <folderPath>")
		}
		storeFolder(os.Args[2])

	case "restore":
		if len(os.Args) < 4 {
			log.Fatal("Usage: capsule restore <fileID> <outputDir>")
		}
		fileID, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			log.Fatal("Invalid fileID: ", err)
		}
		if err := restoreFile(fileID, os.Args[3]); err != nil {
			log.Fatal(err)
		}

	case "remove":
		if len(os.Args) < 3 {
			log.Fatal("Usage: capsule remove <fileID>")
		}
		removeFile(os.Args[2])

	case "gc":
		runGC()

	case "stats":
		runStats()

	default:
		log.Fatal("Unknown command")
	}
}
