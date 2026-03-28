package main

import (
	"fmt"
	"os"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func initCommand() error {
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
	fmt.Println("- ⚠️ Do NOT commit it to git.⚠️ Use environment variables or a secure vault.")
	fmt.Println()

	envPath := ".env"

	if _, err := os.Stat(envPath); err == nil {
		fmt.Println("⚠️  .env file already exists — not modifying it.")
		fmt.Println("Add this manually if needed:")
		fmt.Println()
		fmt.Printf("  COLDKEEP_KEY=%s\n", key)
		fmt.Printf("  COLDKEEP_CODEC=aes-gcm\n")
		return nil
	}

	content := fmt.Sprintf("COLDKEEP_KEY=%s\nCOLDKEEP_CODEC=aes-gcm\n", key)

	if err := os.WriteFile(envPath, []byte(content), 0600); err != nil {
		return fmt.Errorf("write .env: %w", err)
	}

	if isRunningInContainer() {
		fmt.Println("⚠️ Running inside container — .env will not persist unless volume is mounted")
	}
	fmt.Println("✅ .env file created with encryption key")
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
