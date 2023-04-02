package sqlm

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Mysql driver.
	_ "github.com/go-sql-driver/mysql"
)

// MySQLDriver is name of MySQL driver.
const MySQLDriver = "mysql"

const (
	// MaxConn is the maximum number of open connections to the database.
	MaxConn = 25
	// MaxLifetime is the maximum amount of time a connection may be reused.
	MaxLifetime = 5 * time.Minute
	// Timeout is the default timeout duration.
	Timeout = 5 * time.Second
)

// MySQLOpen opens and validates a MySQL database pool of connections.
func MySQLOpen(dataSourceName string) (*sql.DB, error) {
	return Open(MySQLDriver, dataSourceName, MaxConn, MaxLifetime, Timeout)
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a
// database name and connection information.
// It also validates the data source name by calling a Ping.
func Open(driverName, dataSourceName string, maxConn int, maxLifetime, pingTimeout time.Duration) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	db.SetMaxOpenConns(maxConn)
	db.SetMaxIdleConns(maxConn)
	db.SetConnMaxLifetime(maxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}
	return db, nil
}
