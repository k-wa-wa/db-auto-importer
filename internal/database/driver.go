package database

import (
	"database/sql"
	"fmt"
)

// DBClient defines the interface for database operations.
type DBClient interface {
	GetSchemaInfo(schemaName string) (map[string]DBInfo, error)
	PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error)
	ParentRecordExists(dbInfo DBInfo, columnName, value string) (bool, error)
	EnsureParentRecordExists(parentDBInfo DBInfo, foreignColumnName, foreignKeyValue string, dbSchema map[string]DBInfo) error
	GetDB() *sql.DB
	Close() error
}

// NewDBClient creates a new DBClient based on the database type.
func NewDBClient(dbType, connStr string) (DBClient, error) {
	switch dbType {
	case "postgres":
		return NewPostgresDB(connStr)
	case "db2":
		return NewDB2Client(connStr)
	case "mysql":
		return NewMySQLDB(connStr)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}
