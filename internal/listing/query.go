package listing

import (
	"fmt"
	"strconv"
)

func parsePaginationArgs(args []string) (*int64, *int64, error) {
	var limit *int64
	var offset *int64

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--limit":
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("missing argument for --limit")
			}
			i++
			parsedValue, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || parsedValue < 0 {
				return nil, nil, fmt.Errorf("invalid --limit value %q: must be a non-negative integer", args[i])
			}
			limit = &parsedValue
		case "--offset":
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("missing argument for --offset")
			}
			i++
			parsedValue, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil || parsedValue < 0 {
				return nil, nil, fmt.Errorf("invalid --offset value %q: must be a non-negative integer", args[i])
			}
			offset = &parsedValue
		}
	}

	return limit, offset, nil
}

func applyPagination(query string, params []interface{}, paramIndex int, limit, offset *int64) (string, []interface{}) {
	if limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", paramIndex)
		params = append(params, *limit)
		paramIndex++
	}
	if offset != nil {
		query += fmt.Sprintf(" OFFSET $%d", paramIndex)
		params = append(params, *offset)
	}

	return query, params
}
