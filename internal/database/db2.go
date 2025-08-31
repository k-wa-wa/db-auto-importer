package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/ibmdb/go_ibm_db" // DB2 driver
)

// DB2DB implements the DBClient interface for DB2.
type DB2DB struct {
	db *sql.DB
}

// NewDB2DB creates a new DB2DB instance.
func NewDB2DB(connStr string) (*DB2DB, error) {
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
			DataType:      dataType,
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
		fk.TableName = tableName // Set the current table name
		var foreignTableSchema string // Not directly used in ForeignKeyInfo, but needed for scan
		if err := rows.Scan(&fk.ConstraintName, &fk.ColumnName, &foreignTableSchema, &fk.ForeignTableName, &fk.ForeignColumnName); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

// PrepareInsertStatement prepares an INSERT statement for DB2.
func (d *DB2DB) PrepareInsertStatement(dbInfo DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for i, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, "?") // DB2 uses '?' for placeholders
	}

	// DB2 does not have a direct equivalent to PostgreSQL's ON CONFLICT clause.
	// A common approach is to attempt an INSERT and handle duplicates, or
	// check for existence first, then INSERT or UPDATE.
	// For simplicity, we'll use a simple INSERT here. If a primary key conflict occurs,
	// the DB2 driver will return an error, which the importer will log and skip.
	// A more robust solution would involve a MERGE statement or separate SELECT/INSERT/UPDATE logic.

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		dbInfo.TableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := d.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
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
		parentPlaceholders = append(parentPlaceholders, "?") // DB2 uses '?' for placeholders

		var val interface{}
		var err error

		if colInfo.ColumnName == foreignColumnName {
			// Use the foreignKeyValue for the foreign key column that triggered this call
			val, err = convertToDBType(foreignKeyValue, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert foreign key value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", foreignKeyValue, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Use nil if conversion fails
			}
		} else if colInfo.ColumnDefault.Valid {
			// Use the explicit column default if available
			val, err = convertToDBType(colInfo.ColumnDefault.String, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
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
			// For other columns, use default behavior (empty string for convertToDBType)
			val, err = convertToDBType("", colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
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
				err := d.EnsureParentRecordExists(parentOfParentDBInfo, fk.ForeignColumnName, fkValueStr, dbSchema)
				if err != nil {
					return fmt.Errorf("failed to recursively ensure parent record for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValueStr, err)
				}
			}
		} else {
			log.Printf("Warning: Foreign key column '%s' not found in parentDBInfo.Columns for table '%s'. Cannot recursively ensure its parent.\n", fk.ColumnName, parentDBInfo.TableName)
		}
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
