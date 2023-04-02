package sqlm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// BeginTx represents the database connection who handles transactions.
type BeginTx interface {
	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Tx lists all the methods used inside a SQL transaction.
type Tx interface {
	// ExecContext executes a prepared statement with the given arguments and
	// returns a Result summarizing the effect of the statement.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// PrepareContext creates a prepared statement for use within a transaction.
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	// QueryContext executes a query that returns rows, typically a SELECT.
	// The args are for any placeholder parameters in the query.
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// QueryRowContext executes a query that is expected to return at most one row.
	// It always returns a non-nil value.
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// WithTx creates a new transaction and handles commit/rollback based on the returned error.
// It uses db to handle the transaction and ctx to isolate it.
// See https://golang.org/doc/go1.8#database_sql.
func WithTx(ctx context.Context, db BeginTx, f func(Tx) error) (err error) {
	var tx *sql.Tx
	tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() {
		r := recover()
		switch {
		case r != nil:
			// Panic? No. Rollbacks then panics.
			err = errors.Join(fmt.Errorf("%v", r), tx.Rollback())
			panic(err)
		case err != nil:
			err = errors.Join(err, tx.Rollback())
		default:
			err = tx.Commit()
		}
	}()
	return f(tx)
}
