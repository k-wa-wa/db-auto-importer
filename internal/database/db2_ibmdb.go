//go:build ibm_db
// +build ibm_db

package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/ibmdb/go_ibm_db" // DB2 driver
)

// DB2DB implements the DBClient interface for DB2.
type DB2DB struct {
	db *sql.DB
}

// NewDB2Client creates a new DB2DB instance.
func NewDB2Client(connStr string) (DBClient, error) {
	db, err := sql.Open("go_ibm_db", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to DB2 database: %w", err)
	}
	log.Println("Successfully connected to DB2 database.")
	return &DB2DB{db: db}, nil
}

// GetDB returns the underlying *sql.DB connection.
func (d *DB2DB) GetDB() *sql.DB {
	return d.db
}

// Close closes the database connection.
func (d *DB2DB) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetSchemaInfo retrieves schema information for a given schema name from DB2.
func (d *DB2DB) GetSchemaInfo(schemaName string) (map[string]DBInfo, error) {
	log.Printf("Retrieving schema for '%s' from DB2.\n", schemaName)

	tables, err := d.getTableNames(schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table names from schema '%s': %w", schemaName, err)
	}

	schemaInfo := make(map[string]DBInfo)
	for _, tableName := range tables {
		columns, err := d.getColumnInfo(tableName, schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info for table %s: %w", tableName, err)
		}
		primaryKeys, err := d.getPrimaryKeyColumns(tableName, schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key info for table %s: %w", tableName, err)
		}
		uniqueKeys, err := d.getUniqueKeyColumns(tableName, schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get unique key info for table %s: %w", tableName, err)
		}
		foreignKeys, err := d.getForeignKeyInfo(tableName, schemaName)
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

func (d *DB2DB) getTableNames(schemaName string) ([]string, error) {
	rows, err := d.db.Query(`
		SELECT TABNAME
		FROM SYSCAT.TABLES
		WHERE TABSCHEMA = ? AND TYPE = 'T'
	`, strings.ToUpper(schemaName)) // DB2 schema names are typically uppercase
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

func (d *DB2DB) getColumnInfo(tableName, schemaName string) ([]ColumnInfo, error) {
	rows, err := d.db.Query(`
		SELECT COLNAME, TYPENAME, NULLS, DEFAULT
		FROM SYSCAT.COLUMNS
		WHERE TABSCHEMA = ? AND TABNAME = ?
		ORDER BY COLNO
	`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
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
		isNullable := (isNullableStr == "Y") // DB2 uses 'Y' for nullable
		columns = append(columns, ColumnInfo{
			ColumnName:    colName,
			DataType:      ParseDataType(dataType),
			IsNullable:    isNullable,
			ColumnDefault: colDefault,
		})
	}
	return columns, nil
}

func (d *DB2DB) getPrimaryKeyColumns(tableName, schemaName string) ([]string, error) {
	rows, err := d.db.Query(`
		SELECT COLNAME
		FROM SYSCAT.KEYCOLUSE
		WHERE TABSCHEMA = ? AND TABNAME = ? AND CONSTNAME IN (
			SELECT CONSTNAME
			FROM SYSCAT.TABCONST
			WHERE TABSCHEMA = ? AND TABNAME = ? AND TYPE = 'P'
		)
		ORDER BY COLSEQ
	`, strings.ToUpper(schemaName), strings.ToUpper(tableName), strings.ToUpper(schemaName), strings.ToUpper(tableName))
	if err != nil {
		return nil, fmt.Errorf("query failed for table %s: %w", tableName, err)
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

func (d *DB2DB) getUniqueKeyColumns(tableName, schemaName string) ([][]string, error) {
	// DB2 unique key information is a bit more complex to retrieve than PostgreSQL.
	// This query attempts to get unique constraints that are not primary keys.
	rows, err := d.db.Query(`
		SELECT LISTAGG(kcu.COLNAME, ',') WITHIN GROUP (ORDER BY kcu.COLSEQ) AS UNIQUE_COLUMNS
		FROM SYSCAT.KEYCOLUSE kcu
		JOIN SYSCAT.TABCONST tc ON kcu.CONSTNAME = tc.CONSTNAME AND kcu.TABSCHEMA = tc.TABSCHEMA AND kcu.TABNAME = tc.TABNAME
		WHERE kcu.TABSCHEMA = ? AND kcu.TABNAME = ? AND tc.TYPE = 'U'
		GROUP BY kcu.CONSTNAME
	`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
	if err != nil {
		return nil, fmt.Errorf("query failed for table %s: %w", tableName, err)
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

func (d *DB2DB) getForeignKeyInfo(tableName, schemaName string) ([]ForeignKeyInfo, error) {
	rows, err := d.db.Query(`
		SELECT
			rc.CONSTNAME AS CONSTRAINT_NAME,
			kcu.COLNAME AS COLUMN_NAME,
			rc.REFTABSCHEMA AS FOREIGN_TABLE_SCHEMA,
			rc.REFTABNAME AS FOREIGN_TABLE_NAME,
			kcu_ref.COLNAME AS FOREIGN_COLUMN_NAME
		FROM SYSCAT.REFERENCES rc
		JOIN SYSCAT.KEYCOLUSE kcu ON rc.CONSTNAME = kcu.CONSTNAME AND rc.TABSCHEMA = kcu.TABSCHEMA AND rc.TABNAME = kcu.TABNAME
		JOIN SYSCAT.KEYCOLUSE kcu_ref ON rc.REFKEYNAME = kcu_ref.CONSTNAME AND rc.REFTABSCHEMA = kcu_ref.TABSCHEMA AND rc.REFTABNAME = kcu_ref.TABNAME AND kcu.COLSEQ = kcu_ref.COLSEQ
		WHERE rc.TABSCHEMA = ? AND rc.TABNAME = ?
	`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyInfo
	for rows.Next() {
		var fk ForeignKeyInfo
		fk.TableName = tableName      // Set the current table name
		var foreignTableSchema string // Not directly used in ForeignKeyInfo, but needed for scan
		if err := rows.Scan(&fk.ConstraintName, &fk.ColumnName, &foreignTableSchema, &fk.ForeignTableName, &fk.ForeignColumnName); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

// PrepareInsertStatement prepares an UPSERT (MERGE) statement for DB2.
func (d *DB2DB) PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for _, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, "?") // DB2 uses '?' for placeholders
	}

	// If no primary keys are defined, we cannot perform an upsert.
	// In this case, we fall back to a simple INSERT.
	if len(dbInfo.PrimaryKeyColumns) == 0 {
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			dbInfo.TableName,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
		stmt, err := d.db.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare INSERT statement (no primary keys): %w", err)
		}
		return stmt, nil
	}

	// Construct the MERGE statement for upsert
	var mergeOnClauses []string
	for _, pkCol := range dbInfo.PrimaryKeyColumns {
		mergeOnClauses = append(mergeOnClauses, fmt.Sprintf("T.%s = S.%s", pkCol, pkCol))
	}

	var updateSetClauses []string
	var insertCols []string
	var insertValuesFromSource []string
	pkMap := make(map[string]bool)
	for _, pkCol := range dbInfo.PrimaryKeyColumns {
		pkMap[pkCol] = true
	}

	for _, colInfo := range dbInfo.Columns {
		insertCols = append(insertCols, colInfo.ColumnName)
		insertValuesFromSource = append(insertValuesFromSource, fmt.Sprintf("S.%s", colInfo.ColumnName))
		if !pkMap[colInfo.ColumnName] {
			updateSetClauses = append(updateSetClauses, fmt.Sprintf("T.%s = S.%s", colInfo.ColumnName, colInfo.ColumnName))
		}
	}

	// The VALUES clause in the USING part will have one placeholder for each column.
	// The parameters for the prepared statement will correspond to these values.
	// The WHEN MATCHED and WHEN NOT MATCHED clauses will refer to these source values (S.<colname>).
	query := fmt.Sprintf(`
		MERGE INTO %s AS T
		USING (VALUES (%s)) AS S (%s)
		ON (%s)
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s) VALUES (%s)
	`,
		dbInfo.TableName,
		strings.Join(placeholders, ", "), // Placeholders for the VALUES clause
		strings.Join(cols, ", "),         // Column names for the VALUES clause
		strings.Join(mergeOnClauses, " AND "),
		strings.Join(updateSetClauses, ", "),
		strings.Join(insertCols, ", "),
		strings.Join(insertValuesFromSource, ", "),
	)

	stmt, err := d.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare MERGE statement: %w", err)
	}
	return stmt, nil
}

// ParentRecordExists checks if a record exists in the given table for a specific column and value in DB2.
func (d *DB2DB) ParentRecordExists(dbInfo DBInfo, columnName, value string) (bool, error) {
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = ?", dbInfo.TableName, columnName)
	var exists int
	err := d.db.QueryRow(query, value).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check existence of record in %s for %s=%s: %w", dbInfo.TableName, columnName, value, err)
	}
	return true, nil
}

// EnsureParentRecordExists checks if a record with the given foreignKeyValue exists in the parent table.
// If not, it creates a new record in the parent table with default values and the provided foreignKeyValue
// for the foreignColumnName. This implementation is specific to DB2.
func (d *DB2DB) EnsureParentRecordExists(parentDBInfo DBInfo, foreignColumnName, foreignKeyValue string, dbSchema map[string]DBInfo) error {
	// Check if the parent record already exists
	exists, err := d.ParentRecordExists(parentDBInfo, foreignColumnName, foreignKeyValue)
	if err != nil {
		return fmt.Errorf("failed to check parent record existence: %w", err)
	}
	if exists {
		return nil // Parent record already exists
	}

	// Parent record does not exist, create it
	log.Printf("Creating missing parent record in table '%s' for column '%s' with value '%s'\n", parentDBInfo.TableName, foreignColumnName, foreignKeyValue)

	parentCols, _, parentValues, err := ensureParentRecordExistsCommon(d, parentDBInfo, foreignColumnName, foreignKeyValue, dbSchema)
	if err != nil {
		return err
	}

	// Generate DB2-specific placeholders
	parentPlaceholders := make([]string, len(parentCols))
	for i := range parentCols {
		parentPlaceholders[i] = "?"
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		parentDBInfo.TableName,
		strings.Join(parentCols, ", "),
		strings.Join(parentPlaceholders, ", "),
	)

	_, err = d.db.Exec(insertQuery, parentValues...)
	if err != nil {
		return fmt.Errorf("failed to insert parent record into %s: %w", parentDBInfo.TableName, err)
	}

	return nil
}
