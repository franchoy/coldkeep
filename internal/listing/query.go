package listing

import (
	"fmt"
	"strconv"
)

const maxPaginationLimit int64 = 10000

func parseNonNegativeIntArg(flagName, value string) (int64, error) {
	parsedValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsedValue < 0 {
		return 0, fmt.Errorf("invalid --%s value %q: must be a non-negative integer", flagName, value)
	}

	return parsedValue, nil
}

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
			parsedValue, err := parseNonNegativeIntArg("limit", args[i])
			if err != nil {
				return nil, nil, err
			}
			if parsedValue > maxPaginationLimit {
				return nil, nil, fmt.Errorf("invalid --limit value %q: must be <= %d", args[i], maxPaginationLimit)
			}
			value := parsedValue
			limit = &value
		case "--offset":
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("missing argument for --offset")
			}
			i++
			parsedValue, err := parseNonNegativeIntArg("offset", args[i])
			if err != nil {
				return nil, nil, err
			}
			value := parsedValue
			offset = &value
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
