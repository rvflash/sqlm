package sqlm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// Any is map of string containing any values.
type Any map[string]any

// String returns the string representation of the value behind this key.
// If it's a nil pointer, we return a blank string instead.
func (m Any) String(key string) string {
	switch v := m[key].(type) {
	case nil:
		return ""
	case fmt.Stringer:
		return v.String()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// QueryAny executes a query that is expected to return at most one row, so one Any.
// The args are for any placeholder parameters in the query.
// If multiple rows are returned by the query, the Scan method discards all but the first.
func QueryAny(ctx context.Context, conn Tx, query string, args ...any) (Any, error) {
	res, err := queryAnyRows(ctx, conn, query, true, args)
	if err != nil {
		return nil, err
	}
	switch len(res) {
	case 0:
		return nil, sql.ErrNoRows
	default:
		return res[0], nil
	}
}

// QueryAnyRows executes a query that returns rows as slice of Any, typically a SELECT.
// The args are for any placeholder parameters in the query.
func QueryAnyRows(ctx context.Context, conn Tx, query string, args ...any) ([]Any, error) {
	return queryAnyRows(ctx, conn, query, false, args)
}

func queryAnyRows(ctx context.Context, conn Tx, query string, single bool, args []any) ([]Any, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query executing: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("columns describing: %w", err)
	}
	var res []Any
	for rows.Next() {
		rs := makeRs(cols)
		err = rows.Scan(rs...)
		if err != nil {
			return nil, fmt.Errorf("rows scanning: %w", err)
		}
		res = append(res, makeAny(cols, rs))
		if single {
			break
		}
	}
	err = rows.Close()
	if err != nil {
		return nil, fmt.Errorf("rows closing: %w", err)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows iterating: %w", err)
	}
	return res, nil
}

func makeAny(cols []*sql.ColumnType, values []any) Any {
	res := make(Any, len(cols))
	for pos, col := range cols {
		res[col.Name()] = values[pos]
	}
	return res
}

func makeRs(cols []*sql.ColumnType) []any {
	res := make([]any, len(cols))
	for pos, col := range cols {
		res[pos] = reflect.Zero(col.ScanType())
	}
	return res
}
