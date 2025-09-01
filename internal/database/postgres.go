package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

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
			DataType:      ParseDataType(dataType),
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
		log.Printf("DEBUG: Found foreign key: %+v\n", fk) // Add debug log
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

	parentCols, _, parentValues, err := ensureParentRecordExistsCommon(p, parentDBInfo, foreignColumnName, foreignKeyValue, dbSchema)
	if err != nil {
		return err
	}

	// Generate PostgreSQL-specific placeholders
	parentPlaceholders := make([]string, len(parentCols))
	for i := range parentCols {
		parentPlaceholders[i] = fmt.Sprintf("$%d", i+1)
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
