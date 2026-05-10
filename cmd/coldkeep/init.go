package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

func initCommand(parsed parsedCommandLine, outputMode cliOutputMode) error {
	_ = outputMode

	key, err := blocks.GenerateKeyHex()
	if err != nil {
		return err
	}

	fmt.Println("🔐 Coldkeep aes-gcm Encryption Initialization")
	fmt.Println()
	fmt.Println("Generated aes-gcm encryption key:")
	fmt.Println()
	fmt.Printf("  %s\n\n", key)

	fmt.Println("IMPORTANT:")
	fmt.Println("- Store this key safely.")
	fmt.Println("- If you lose it, your data cannot be recovered.")
	fmt.Println("- ⚠️ Do NOT commit it to git. ⚠️ Use environment variables or a secure vault.")
	fmt.Println()

	envPath := ".env"
	// Handle compression configuration if specified
	compressionCodec := ""
	compressionLevel := 0
	if compFlag, exists := parsed.lastFlagValue("compression"); exists {
		compFlag = strings.TrimSpace(compFlag)
		if !storage.IsRegisteredCompressionCodec(compFlag) {
			return fmt.Errorf("invalid compression codec %q", compFlag)
		}
		compressionCodec = compFlag
		fmt.Println("📦 Compression Configuration")
		fmt.Println()
		fmt.Printf("  Compression codec: %s\n", compressionCodec)
	}
	if levelFlag, exists := parsed.lastFlagValue("compression-level"); exists {
		level, err := strconv.Atoi(levelFlag)
		if err != nil {
			return fmt.Errorf("invalid compression-level: %v", err)
		}
		if level < 1 || level > 9 {
			return fmt.Errorf("invalid compression-level %d: must be in range [1, 9]", level)
		}
		compressionLevel = level
		fmt.Printf("  Compression level: %d\n", level)
	}
	if compressionCodec == "" && compressionLevel > 0 {
		return fmt.Errorf("compression-level requires --compression zstd")
	}
	if compressionCodec == storagecompression.CompressionNone && compressionLevel > 0 {
		return fmt.Errorf("compression-level is not applicable when compression codec is %q", storagecompression.CompressionNone)
	}

	if _, err := os.Stat(envPath); err == nil {
		fmt.Println("⚠️  .env file already exists — not modifying it.")
		fmt.Println("Add this manually if needed:")
		fmt.Println()
		fmt.Printf("  COLDKEEP_KEY=%s\n", key)
		fmt.Printf("  COLDKEEP_CODEC=aes-gcm\n")
		if compressionCodec != "" {
			fmt.Printf("  COLDKEEP_COMPRESSION=%s\n", compressionCodec)
			if compressionLevel > 0 {
				fmt.Printf("  COLDKEEP_COMPRESSION_LEVEL=%d\n", compressionLevel)
			}
		}
		if compressionCodec != "" {
			fmt.Printf("✅ Compression configured: %s", compressionCodec)
			if compressionLevel > 0 {
				fmt.Printf(" (level %d)", compressionLevel)
			}
			fmt.Println()
			fmt.Println("ℹ️  Compression note: Applies to NEW blocks only. Existing blocks are not modified.")
		}
		return nil
	}

	content := fmt.Sprintf("COLDKEEP_KEY=%s\nCOLDKEEP_CODEC=aes-gcm\n", key)
	if compressionCodec != "" {
		content += fmt.Sprintf("COLDKEEP_COMPRESSION=%s\n", compressionCodec)
		if compressionLevel > 0 {
			content += fmt.Sprintf("COLDKEEP_COMPRESSION_LEVEL=%d\n", compressionLevel)
		}
	}

	if err := os.WriteFile(envPath, []byte(content), 0600); err != nil {
		return fmt.Errorf("write .env: %w", err)
	}

	if isRunningInContainer() {
		fmt.Println("⚠️ Running inside container — .env will not persist unless volume is mounted")
	}
	fmt.Println("✅ .env file created with encryption key")
	if compressionCodec != "" {
		fmt.Printf("✅ Compression configured: %s", compressionCodec)
		if compressionLevel > 0 {
			fmt.Printf(" (level %d)", compressionLevel)
		}
		fmt.Println()
		fmt.Println("ℹ️  Compression note: Applies to NEW blocks only. Existing blocks are not modified.")
	}
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  export $(cat .env | xargs)")
	fmt.Println("  coldkeep store file.txt")

	return nil
}

func isRunningInContainer() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}

// checkEnvFilePermissions warns if .env exists with permissions other than 0600.
func checkEnvFilePermissions() {
	info, err := os.Stat(".env")
	if err != nil {
		return // .env does not exist, nothing to check
	}
	if info.Mode().Perm() != 0600 {
		fmt.Fprintf(os.Stderr, "WARNING: .env has permissions %s — should be 0600. Run: chmod 0600 .env\n", info.Mode().Perm())
	}
}
