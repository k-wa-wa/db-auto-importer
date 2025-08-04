package database

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/lib/pq" // PostgreSQL driver
)

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

// GetSchemaInfo connects to the PostgreSQL database and retrieves schema information.
func GetSchemaInfo(connStr string) (map[string]DBInfo, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Println("Successfully connected to the database.")

	tables, err := getTableNames(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get table names: %w", err)
	}

	schemaInfo := make(map[string]DBInfo)
	for _, tableName := range tables {
		columns, err := getColumnInfo(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info for table %s: %w", tableName, err)
		}
		primaryKeys, err := getPrimaryKeyColumns(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key info for table %s: %w", tableName, err)
		}
		uniqueKeys, err := getUniqueKeyColumns(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get unique key info for table %s: %w", tableName, err)
		}
		foreignKeys, err := getForeignKeyInfo(db, tableName)
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

func getTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
	`)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
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

func getColumnInfo(db *sql.DB, tableName string) ([]ColumnInfo, error) {
	rows, err := db.Query(`
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position;
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
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

func getPrimaryKeyColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(`
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

func getUniqueKeyColumns(db *sql.DB, tableName string) ([][]string, error) {
	rows, err := db.Query(`
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

func getForeignKeyInfo(db *sql.DB, tableName string) ([]ForeignKeyInfo, error) {
	rows, err := db.Query(`
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
