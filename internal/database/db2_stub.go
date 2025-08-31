//go:build !ibm_db
// +build !ibm_db

package database

import (
	"database/sql"
	"fmt"
)

// newDB2Client returns an error indicating that DB2 support is not compiled.
func newDB2Client(connStr string) (DBClient, error) {
	return nil, fmt.Errorf("DB2 support not compiled. Build with -tags ibm_db to enable")
}

// These are stub implementations to satisfy the DBClient interface when ibm_db is not built.
type stubDB2Client struct{}

func (s *stubDB2Client) GetSchemaInfo(schemaName string) (map[string]DBInfo, error) {
	return nil, fmt.Errorf("DB2 support not compiled")
}
func (s *stubDB2Client) PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error) {
	return nil, fmt.Errorf("DB2 support not compiled")
}
func (s *stubDB2Client) ParentRecordExists(dbInfo DBInfo, columnName, value string) (bool, error) {
	return false, fmt.Errorf("DB2 support not compiled")
}
func (s *stubDB2Client) EnsureParentRecordExists(parentDBInfo DBInfo, foreignColumnName, foreignKeyValue string, dbSchema map[string]DBInfo) error {
	return fmt.Errorf("DB2 support not compiled")
}
func (s *stubDB2Client) GetDB() *sql.DB {
	return nil
}
func (s *stubDB2Client) Close() error {
	return nil
}
