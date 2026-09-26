package storage

import (
	"context"
)

type QuerySqlResponse struct {
	Columns []string `json:"columns"`
	Results [][]any  `json:"results"`
}

func QuerySql(ctx context.Context, s *Storage, query string) (*QuerySqlResponse, error) {
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names.
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &QuerySqlResponse{
		Columns: columns,
		Results: [][]any{},
	}

	// Prepare pointers for Scanning.
	// values: holds the actual data
	// valuePtrs: holds pointers to the values so Scan can modify them
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		// Scan into the pointer slice
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Process the row.
		rowResult := make([]any, len(columns))
		for i, val := range values {
			// Database drivers often return strings/numbers as []byte.
			// To make this JSON friendly, we convert []byte to string.
			if b, ok := val.([]byte); ok {
				rowResult[i] = string(b)
			} else {
				rowResult[i] = val
			}
		}

		// Add the row to our main results set
		result.Results = append(result.Results, rowResult)
	}

	return result, nil
}
