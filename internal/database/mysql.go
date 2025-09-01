package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// MySQLDB implements the DBClient interface for MySQL.
type MySQLDB struct {
	db *sql.DB
}

// NewMySQLDB creates a new MySQLDB instance.
func NewMySQLDB(connStr string) (*MySQLDB, error) {
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to MySQL database: %w", err)
	}
	log.Println("Successfully connected to MySQL database.")
	return &MySQLDB{db: db}, nil
}

// GetDB returns the underlying *sql.DB connection.
func (m *MySQLDB) GetDB() *sql.DB {
	return m.db
}

// Close closes the database connection.
func (m *MySQLDB) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// GetSchemaInfo retrieves schema information for a given database name from MySQL.
func (m *MySQLDB) GetSchemaInfo(dbName string) (map[string]DBInfo, error) {
	log.Printf("Retrieving schema for '%s' from MySQL.\n", dbName)

	tables, err := m.getTableNames(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table names from database '%s': %w", dbName, err)
	}

	schemaInfo := make(map[string]DBInfo)
	for _, tableName := range tables {
		columns, err := m.getColumnInfo(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info for table %s: %w", tableName, err)
		}
		primaryKeys, err := m.getPrimaryKeyColumns(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key info for table %s: %w", tableName, err)
		}
		uniqueKeys, err := m.getUniqueKeyColumns(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get unique key info for table %s: %w", tableName, err)
		}
		foreignKeys, err := m.getForeignKeyInfo(dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get foreign key info for table %s: %w", tableName, err)
		}

		schemaInfo[tableName] = DBInfo{
			TableName:         tableName,
			Columns:           columns,
			PrimaryKeyColumns: primaryKeys,
			UniqueKeyColumns:  uniqueKeys,
			ForeignKeys:       foreignKeys,
		}
	}

	return schemaInfo, nil
}

func (m *MySQLDB) getTableNames(dbName string) ([]string, error) {
	rows, err := m.db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE';
	`, dbName)
	if err != nil {
		return nil, fmt.Errorf("query failed for database '%s': %w", dbName, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (m *MySQLDB) getColumnInfo(dbName, tableName string) ([]ColumnInfo, error) {
	rows, err := m.db.Query(`
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position;
	`, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed for table %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var colName, dataType, isNullableStr string
		var colDefault sql.NullString
		if err := rows.Scan(&colName, &dataType, &isNullableStr, &colDefault); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		isNullable := (isNullableStr == "YES")
		columns = append(columns, ColumnInfo{
			ColumnName:    colName,
			DataType:      ParseDataType(dataType),
			IsNullable:    isNullable,
			ColumnDefault: colDefault,
		})
	}
	return columns, nil
}

func (m *MySQLDB) getPrimaryKeyColumns(dbName, tableName string) ([]string, error) {
	rows, err := m.db.Query(`
		SELECT DISTINCT kcu.column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		AND tc.table_schema = kcu.table_schema
		AND tc.table_name = kcu.table_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_schema = ? AND tc.table_name = ?;
	`, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pkCol string
		if err := rows.Scan(&pkCol); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		pks = append(pks, pkCol)
	}
	return pks, nil
}

func (m *MySQLDB) getUniqueKeyColumns(dbName, tableName string) ([][]string, error) {
	rows, err := m.db.Query(`
		SELECT GROUP_CONCAT(DISTINCT kcu.column_name ORDER BY kcu.ordinal_position)
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'UNIQUE'
		AND tc.table_schema = ? AND tc.table_name = ?
		GROUP BY tc.constraint_name;
	`, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var uks [][]string
	for rows.Next() {
		var uniqueColsStr string
		if err := rows.Scan(&uniqueColsStr); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		uks = append(uks, strings.Split(uniqueColsStr, ","))
	}
	return uks, nil
}

func (m *MySQLDB) getForeignKeyInfo(dbName, tableName string) ([]ForeignKeyInfo, error) {
	rows, err := m.db.Query(`
		SELECT
			kcu.constraint_name,
			kcu.column_name,
			kcu.referenced_table_name AS foreign_table_name,
			kcu.referenced_column_name AS foreign_column_name
		FROM
			information_schema.key_column_usage AS kcu
		WHERE
			kcu.constraint_schema = ?
			AND kcu.table_name = ?
			AND kcu.referenced_table_name IS NOT NULL;
	`, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyInfo
	for rows.Next() {
		var fk ForeignKeyInfo
		fk.TableName = tableName // Set the current table name
		if err := rows.Scan(&fk.ConstraintName, &fk.ColumnName, &fk.ForeignTableName, &fk.ForeignColumnName); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		log.Printf("DEBUG: Found foreign key: %+v\n", fk) // Add debug log
		fks = append(fks, fk)
	}
	return fks, nil
}

// PrepareInsertStatement prepares an INSERT statement for MySQL.
func (m *MySQLDB) PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for _, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, "?")
	}

	pkMap := make(map[string]bool)
	for _, pkCol := range dbInfo.PrimaryKeyColumns {
		pkMap[pkCol] = true
	}

	var query string
	if len(dbInfo.PrimaryKeyColumns) > 0 {
		var updateClauses []string
		for _, colInfo := range dbInfo.Columns {
			if !pkMap[colInfo.ColumnName] {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = VALUES(%s)", colInfo.ColumnName, colInfo.ColumnName))
			}
		}

		if len(updateClauses) > 0 {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
				dbInfo.TableName,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(updateClauses, ", "),
			)
		} else {
			// If only primary keys are present, and no other columns to update,
			// use INSERT IGNORE to prevent errors on duplicate primary keys.
			query = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)",
				dbInfo.TableName,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "),
			)
		}
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			dbInfo.TableName,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	}

	stmt, err := m.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return stmt, nil
}

// ParentRecordExists checks if a record exists in the given table for a specific column and value in MySQL.
func (m *MySQLDB) ParentRecordExists(dbInfo DBInfo, columnName, value string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ?)", dbInfo.TableName, columnName)
	var exists bool
	err := m.db.QueryRow(query, value).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of record in %s for %s=%s: %w", dbInfo.TableName, columnName, value, err)
	}
	return exists, nil
}

// EnsureParentRecordExists checks if a record with the given foreignKeyValue exists in the parent table.
// If not, it creates a new record in the parent table with default values and the provided foreignKeyValue
// for the foreignColumnName. This implementation is specific to MySQL.
func (m *MySQLDB) EnsureParentRecordExists(parentDBInfo DBInfo, foreignColumnName, foreignKeyValue string, dbSchema map[string]DBInfo) error {
	// Check if the parent record already exists
	exists, err := m.ParentRecordExists(parentDBInfo, foreignColumnName, foreignKeyValue)
	if err != nil {
		return fmt.Errorf("failed to check parent record existence: %w", err)
	}
	if exists {
		return nil // Parent record already exists
	}

	// Parent record does not exist, create it
	log.Printf("Creating missing parent record in table '%s' for column '%s' with value '%s'\n", parentDBInfo.TableName, foreignColumnName, foreignKeyValue)

	parentCols, _, parentValues, err := ensureParentRecordExistsCommon(m, parentDBInfo, foreignColumnName, foreignKeyValue, dbSchema)
	if err != nil {
		return err
	}

	// Generate MySQL-specific placeholders
	parentPlaceholders := make([]string, len(parentCols))
	for i := range parentCols {
		parentPlaceholders[i] = "?"
	}

	insertQuery := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)",
		parentDBInfo.TableName,
		strings.Join(parentCols, ", "),
		strings.Join(parentPlaceholders, ", "),
	)

	_, err = m.db.Exec(insertQuery, parentValues...)
	if err != nil {
		return fmt.Errorf("failed to insert parent record into %s: %w", parentDBInfo.TableName, err)
	}

	return nil
}
