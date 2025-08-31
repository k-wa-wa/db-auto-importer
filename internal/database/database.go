package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv" // Added for string conversions
	"strings"
	"time" // For time.Time in EnsureParentRecordExists

	"github.com/lib/pq" // PostgreSQL driver
)

// PostgresDB implements the DBClient interface for PostgreSQL.
type PostgresDB struct {
	db *sql.DB
}

// NewPostgresDB creates a new PostgresDB instance.
func NewPostgresDB(connStr string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to PostgreSQL database: %w", err)
	}
	log.Println("Successfully connected to PostgreSQL database.")
	return &PostgresDB{db: db}, nil
}

// GetDB returns the underlying *sql.DB connection.
func (p *PostgresDB) GetDB() *sql.DB {
	return p.db
}

// Close closes the database connection.
func (p *PostgresDB) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// DBInfo holds information about a database table and its columns.
type DBInfo struct {
	TableName         string
	Columns           []ColumnInfo
	PrimaryKeyColumns []string
	UniqueKeyColumns  [][]string
	ForeignKeys       []ForeignKeyInfo
}

// ColumnInfo holds information about a database column.
type ColumnInfo struct {
	ColumnName    string
	DataType      string
	IsNullable    bool
	ColumnDefault sql.NullString
}

// ForeignKeyInfo holds information about a foreign key constraint.
type ForeignKeyInfo struct {
	ConstraintName    string
	TableName         string
	ColumnName        string
	ForeignTableName  string
	ForeignColumnName string
}

// GetSchemaInfo retrieves schema information for a given schema name from PostgreSQL.
func (p *PostgresDB) GetSchemaInfo(schemaName string) (map[string]DBInfo, error) {
	log.Printf("Retrieving schema for '%s' from PostgreSQL.\n", schemaName)

	tables, err := p.getTableNames(schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table names from schema '%s': %w", schemaName, err)
	}

	schemaInfo := make(map[string]DBInfo)
	for _, tableName := range tables {
		columns, err := p.getColumnInfo(tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info for table %s: %w", tableName, err)
		}
		primaryKeys, err := p.getPrimaryKeyColumns(tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key info for table %s: %w", tableName, err)
		}
		uniqueKeys, err := p.getUniqueKeyColumns(tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get unique key info for table %s: %w", tableName, err)
		}
		foreignKeys, err := p.getForeignKeyInfo(tableName)
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

func (p *PostgresDB) getTableNames(schemaName string) ([]string, error) {
	rows, err := p.db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE';
	`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("query failed for schema '%s': %w", schemaName, err)
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

func (p *PostgresDB) getColumnInfo(tableName string) ([]ColumnInfo, error) {
	rows, err := p.db.Query(`
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position;
	`, tableName)
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
			DataType:      dataType,
			IsNullable:    isNullable,
			ColumnDefault: colDefault,
		})
	}
	return columns, nil
}

func (p *PostgresDB) getPrimaryKeyColumns(tableName string) ([]string, error) {
	rows, err := p.db.Query(`
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary;
	`, tableName)
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

func (p *PostgresDB) getUniqueKeyColumns(tableName string) ([][]string, error) {
	rows, err := p.db.Query(`
		SELECT
			array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) AS unique_columns
		FROM
			pg_index i
		JOIN
			pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE
			i.indrelid = $1::regclass
			AND i.indisunique
			AND NOT i.indisprimary -- Exclude primary keys, as they are already unique
		GROUP BY
			i.indexrelid;
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var uks [][]string
	for rows.Next() {
		var uniqueCols []string
		if err := rows.Scan(pq.Array(&uniqueCols)); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		uks = append(uks, uniqueCols)
	}
	return uks, nil
}

func (p *PostgresDB) getForeignKeyInfo(tableName string) ([]ForeignKeyInfo, error) {
	rows, err := p.db.Query(`
		SELECT
			tc.constraint_name,
			kcu.column_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM
			information_schema.table_constraints AS tc
		JOIN
			information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN
			information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.table_schema = tc.table_schema
		WHERE
			tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;
	`, tableName)
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
		fks = append(fks, fk)
	}
	return fks, nil
}

// PrepareInsertStatement prepares an INSERT statement for PostgreSQL.
func (p *PostgresDB) PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for i, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
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
				updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", colInfo.ColumnName, colInfo.ColumnName))
			}
		}

		if len(updateClauses) > 0 {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
				dbInfo.TableName,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(dbInfo.PrimaryKeyColumns, ", "),
				strings.Join(updateClauses, ", "),
			)
		} else {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
				dbInfo.TableName,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(dbInfo.PrimaryKeyColumns, ", "),
			)
		}
	} else {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			dbInfo.TableName,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	}

	stmt, err := p.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return stmt, nil
}

// ParentRecordExists checks if a record exists in the given table for a specific column and value in PostgreSQL.
func (p *PostgresDB) ParentRecordExists(dbInfo DBInfo, columnName, value string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = $1)", dbInfo.TableName, columnName)
	var exists bool
	err := p.db.QueryRow(query, value).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of record in %s for %s=%s: %w", dbInfo.TableName, columnName, value, err)
	}
	return exists, nil
}

// EnsureParentRecordExists checks if a record with the given foreignKeyValue exists in the parent table.
// If not, it creates a new record in the parent table with default values and the provided foreignKeyValue
// for the foreignColumnName. This implementation is specific to PostgreSQL.
func (p *PostgresDB) EnsureParentRecordExists(parentDBInfo DBInfo, foreignColumnName, foreignKeyValue string, dbSchema map[string]DBInfo) error {
	// Check if the parent record already exists
	exists, err := p.ParentRecordExists(parentDBInfo, foreignColumnName, foreignKeyValue)
	if err != nil {
		return fmt.Errorf("failed to check parent record existence: %w", err)
	}
	if exists {
		return nil // Parent record already exists
	}

	// Parent record does not exist, create it
	log.Printf("Creating missing parent record in table '%s' for column '%s' with value '%s'\n", parentDBInfo.TableName, foreignColumnName, foreignKeyValue)

	// Prepare values for the new parent record
	parentCols := make([]string, 0, len(parentDBInfo.Columns))
	parentPlaceholders := make([]string, 0, len(parentDBInfo.Columns))
	parentValues := make([]interface{}, len(parentDBInfo.Columns)) // Initialize with correct size

	// Create a map for quick lookup of unique key columns (including primary keys)
	uniqueColsMap := make(map[string]bool)
	for _, pkCol := range parentDBInfo.PrimaryKeyColumns {
		uniqueColsMap[pkCol] = true
	}
	for _, ukCols := range parentDBInfo.UniqueKeyColumns {
		if len(ukCols) == 1 { // Only consider single-column unique constraints for random generation
			uniqueColsMap[ukCols[0]] = true
		}
	}

	// First, populate parentValues with default/provided/random values
	for colIdx, colInfo := range parentDBInfo.Columns {
		parentCols = append(parentCols, colInfo.ColumnName)
		parentPlaceholders = append(parentPlaceholders, fmt.Sprintf("$%d", colIdx+1))

		var val interface{}
		var err error

		if colInfo.ColumnName == foreignColumnName {
			// Use the foreignKeyValue for the foreign key column that triggered this call
			val, err = ConvertToDBType(foreignKeyValue, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert foreign key value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", foreignKeyValue, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Use nil if conversion fails
			}
		} else if colInfo.ColumnDefault.Valid {
			// Use the explicit column default if available
			val, err = ConvertToDBType(colInfo.ColumnDefault.String, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert default value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", colInfo.ColumnDefault.String, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil
			}
		} else if uniqueColsMap[colInfo.ColumnName] && !colInfo.IsNullable {
			// If it's a unique column (PK or UK) and not nullable, generate a random value
			val, err = generateRandomValue(colInfo.DataType)
			if err != nil {
				log.Printf("Warning: Failed to generate random value for unique column %s (%s) in parent table %s: %v. Using nil.\n", colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Fallback to nil if random generation fails
			}
		} else {
			// For other columns, use default behavior (empty string for ConvertToDBType)
			val, err = ConvertToDBType("", colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to get default value for column %s (%s) in parent table %s: %v. Using nil.\n", colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Use nil if conversion fails
			}
		}
		parentValues[colIdx] = val
	}

	// Recursively ensure parent records for this parentDBInfo's foreign keys
	for _, fk := range parentDBInfo.ForeignKeys {
		// Find the value for this foreign key from the prepared parentValues
		fkColIdx := -1
		for idx, colInfo := range parentDBInfo.Columns {
			if colInfo.ColumnName == fk.ColumnName {
				fkColIdx = idx
				break
			}
		}

		if fkColIdx != -1 {
			fkValueInterface := parentValues[fkColIdx] // This is an interface{}
			if fkValueInterface != nil {
				// Convert the interface{} value back to a string suitable for the recursive call
				var fkValueStr string
				switch v := fkValueInterface.(type) {
				case int64:
					fkValueStr = strconv.FormatInt(v, 10)
				case float64:
					fkValueStr = strconv.FormatFloat(v, 'f', -1, 64)
				case bool:
					fkValueStr = strconv.FormatBool(v)
				case time.Time:
					fkValueStr = v.Format(time.RFC3339) // Or another suitable format
				case string:
					fkValueStr = v
				default:
					// Fallback for other types, might need more specific handling
					fkValueStr = fmt.Sprintf("%v", v)
				}

				parentOfParentDBInfo, ok := dbSchema[fk.ForeignTableName]
				if !ok {
					return fmt.Errorf("foreign table %s not found in schema info for foreign key %s during recursive ensureParent", fk.ForeignTableName, fk.ConstraintName)
				}
				err := p.EnsureParentRecordExists(parentOfParentDBInfo, fk.ForeignColumnName, fkValueStr, dbSchema)
				if err != nil {
					return fmt.Errorf("failed to recursively ensure parent record for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValueStr, err)
				}
			}
		} else {
			log.Printf("Warning: Foreign key column '%s' not found in parentDBInfo.Columns for table '%s'. Cannot recursively ensure its parent.\n", fk.ColumnName, parentDBInfo.TableName)
		}
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		parentDBInfo.TableName,
		strings.Join(parentCols, ", "),
		strings.Join(parentPlaceholders, ", "),
	)
	// TODO: Consider UPSERT for parent record creation if primary key might conflict

	_, err = p.db.Exec(insertQuery, parentValues...)
	if err != nil {
		return fmt.Errorf("failed to insert parent record into %s: %w", parentDBInfo.TableName, err)
	}

	return nil
}
