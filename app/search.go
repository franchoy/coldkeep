package main

import (
	"fmt"
	"log"
	"time"
)

func searchFiles(args []string) error {
	db, err := connectDB()
	if err != nil {
		log.Fatal("Failed to connect to DB:", err)
		return err
	}
	defer db.Close()

	query := `
		SELECT id, original_name, total_size, created_at
		FROM logical_file
		WHERE 1=1
	`
	var params []interface{}
	paramIndex := 1

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			i++
			query += fmt.Sprintf(" AND original_name ILIKE $%d", paramIndex)
			params = append(params, "%"+args[i]+"%")
			paramIndex++

		case "--min-size":
			i++
			query += fmt.Sprintf(" AND total_size >= $%d", paramIndex)
			params = append(params, args[i])
			paramIndex++

		case "--max-size":
			i++
			query += fmt.Sprintf(" AND total_size <= $%d", paramIndex)
			params = append(params, args[i])
			paramIndex++
		}
	}

	query += " ORDER BY created_at DESC"

	rows, err := db.Query(query, params...)
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
