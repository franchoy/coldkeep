package main

import (
	"fmt"
	"time"
)

func listFiles() error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT id, original_name, total_size, created_at
		FROM logical_file
		ORDER BY created_at DESC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Printf("%-6s %-25s %-15s %-20s\n", "ID", "NAME", "SIZE(bytes)", "CREATED_AT")
	fmt.Println("---------------------------------------------------------------------")

	for rows.Next() {
		var id int64
		var name string
		var size int64
		var created time.Time

		if err := rows.Scan(&id, &name, &size, &created); err != nil {
			return err
		}

		fmt.Printf("%-6d %-25s %-15d %-20s\n",
			id,
			name,
			size,
			created.Format("2006-01-02 15:04:05"),
		)
	}
	return nil
}
