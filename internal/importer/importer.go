package importer

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"db-auto-importer/internal/database"
	"db-auto-importer/internal/graph"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// Importer handles the CSV parsing and data import logic.
type Importer struct {
	DBSchema  map[string]database.DBInfo
	DBConnStr string
	db        *sql.DB // Database connection
}

// NewImporter creates a new Importer instance.
func NewImporter(dbSchema map[string]database.DBInfo, dbConnStr string) (*Importer, error) {
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	log.Println("Importer successfully connected to the database.")

	return &Importer{
		DBSchema:  dbSchema,
		DBConnStr: dbConnStr,
		db:        db,
	}, nil
}

// Close closes the database connection.
func (i *Importer) Close() error {
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}

// ImportCSVFiles reads CSV files from the given directory and imports them into the database.
func (i *Importer) ImportCSVFiles(csvDir string) error {
	csvFilesMap := make(map[string]string) // Map table name to CSV file path
	files, err := getCSVFiles(csvDir)
	if err != nil {
		return fmt.Errorf("failed to get CSV files from %s: %w", csvDir, err)
	}
	for _, filePath := range files {
		tableName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
		csvFilesMap[tableName] = filePath
	}

	// Determine import order based on foreign key constraints
	dependencyGraph := graph.NewGraph(i.DBSchema)
	importOrder, err := dependencyGraph.TopologicalSort()
	if err != nil {
		return fmt.Errorf("failed to determine import order: %w", err)
	}

	fmt.Printf("Determined import order: %v\n", importOrder)

	for _, tableName := range importOrder {
		filePath, ok := csvFilesMap[tableName]
		if !ok {
			fmt.Printf("Skipping table %s: no corresponding CSV file found.\n", tableName)
			continue
		}

		dbInfo, ok := i.DBSchema[tableName]
		if !ok {
			// This should ideally not happen if schemaInfo is consistent with graph nodes
			fmt.Printf("Skipping table %s: no corresponding table found in database schema info.\n", tableName)
			continue
		}

		fmt.Printf("Importing data from %s into table %s...\n", filePath, tableName)
		// For now, assume CSVs always have headers. This can be made configurable later.
		if err := i.ImportSingleCSV(filePath, dbInfo, true); err != nil {
			return fmt.Errorf("failed to import %s: %w", filePath, err)
		}
		fmt.Printf("Finished importing %s.\n", filePath)
	}

	return nil
}

func (i *Importer) ImportSingleCSV(filePath string, dbInfo database.DBInfo, hasHeader bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var csvHeader []string
	if hasHeader {
		csvHeader, err = reader.Read() // Read header row
		if err != nil {
			return fmt.Errorf("failed to read CSV header from %s: %w", filePath, err)
		}
	}

	// Map CSV columns to database columns
	columnMap := make(map[string]int) // Maps DB column name to CSV column index
	if hasHeader {
		for _, colInfo := range dbInfo.Columns {
			found := false
			for csvIdx, csvColName := range csvHeader {
				if strings.EqualFold(colInfo.ColumnName, csvColName) {
					columnMap[colInfo.ColumnName] = csvIdx
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("Warning: Column '%s' in table '%s' not found in CSV header. Will use default/null.\n", colInfo.ColumnName, dbInfo.TableName)
			}
		}
	} else {
		// If no header, assume CSV columns are in the same order as DB columns
		for idx, colInfo := range dbInfo.Columns {
			columnMap[colInfo.ColumnName] = idx
		}
	}

	tx, err := i.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	stmt, err := prepareInsertStatement(tx, dbInfo) // columnMap is no longer needed here
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement for table %s: %w", dbInfo.TableName, err)
	}
	defer stmt.Close()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record from %s: %w", filePath, err)
		}

		// Prepare values for insertion
		values := make([]interface{}, len(dbInfo.Columns))
		for colIdx, colInfo := range dbInfo.Columns {
			csvVal := ""
			if idx, ok := columnMap[colInfo.ColumnName]; ok && idx < len(record) {
				csvVal = record[idx]
			} else if !ok && !hasHeader {
				// If no header, and column not found in map (shouldn't happen if columnMap is populated correctly by index)
				// This case might occur if the CSV has fewer columns than the DB table.
				// In such cases, we should use default/null for missing columns.
				// For now, let's assume the CSV has at least as many columns as the DB expects for mapped columns.
				// If the column is not in the map, it means it's not expected from CSV, so it should be handled by default/null.
				// This logic needs to be robust for cases where CSV has fewer columns than DB.
				// For now, if columnMap doesn't contain the column, csvVal remains empty, and convertToDBType handles default/null.
			}

			// Check for foreign key constraints and ensure parent records exist
			for _, fk := range dbInfo.ForeignKeys {
				if fk.ColumnName == colInfo.ColumnName {
					parentDBInfo, ok := i.DBSchema[fk.ForeignTableName]
					if !ok {
						return fmt.Errorf("foreign table %s not found in schema info for foreign key %s", fk.ForeignTableName, fk.ConstraintName)
					}

					fkValue := csvVal
					err := i.ensureParentRecordExists(tx, parentDBInfo, fk.ForeignColumnName, fkValue)
					if err != nil {
						return fmt.Errorf("failed to ensure parent record exists for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValue, err)
					}
					break
				}
			}

			convertedVal, err := convertToDBType(csvVal, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				return fmt.Errorf("failed to convert value for column %s (%s): %w", colInfo.ColumnName, colInfo.DataType, err)
			}
			values[colIdx] = convertedVal
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to insert record into %s: %w", dbInfo.TableName, err)
		}
	}

	return tx.Commit()
}

// ensureParentRecordExists checks if a record with the given foreignKeyValue exists in the parent table.
// If not, it creates a new record in the parent table with default values and the provided foreignKeyValue
// for the foreignColumnName.
func (i *Importer) ensureParentRecordExists(tx *sql.Tx, parentDBInfo database.DBInfo, foreignColumnName, foreignKeyValue string) error {
	// Check if the parent record already exists
	exists, err := i.parentRecordExists(tx, parentDBInfo, foreignColumnName, foreignKeyValue)
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
	parentValues := make([]interface{}, 0, len(parentDBInfo.Columns))

	for idx, colInfo := range parentDBInfo.Columns {
		parentCols = append(parentCols, colInfo.ColumnName)
		parentPlaceholders = append(parentPlaceholders, fmt.Sprintf("$%d", idx+1))

		var val interface{}
		if colInfo.ColumnName == foreignColumnName {
			// Use the foreignKeyValue for the foreign key column
			val, err = convertToDBType(foreignKeyValue, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				return fmt.Errorf("failed to convert foreign key value '%s' for column %s (%s): %w", foreignKeyValue, colInfo.ColumnName, colInfo.DataType, err)
			}
		} else {
			// Use default values for other columns
			val, err = convertToDBType("", colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				return fmt.Errorf("failed to get default value for column %s (%s): %w", colInfo.ColumnName, colInfo.DataType, err)
			}
		}
		parentValues = append(parentValues, val)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		parentDBInfo.TableName,
		strings.Join(parentCols, ", "),
		strings.Join(parentPlaceholders, ", "),
	)
	// TODO: Consider UPSERT for parent record creation if primary key might conflict

	_, err = tx.Exec(insertQuery, parentValues...)
	if err != nil {
		return fmt.Errorf("failed to insert parent record into %s: %w", parentDBInfo.TableName, err)
	}

	return nil
}

// parentRecordExists checks if a record exists in the given table for a specific column and value.
func (i *Importer) parentRecordExists(tx *sql.Tx, dbInfo database.DBInfo, columnName, value string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = $1)", dbInfo.TableName, columnName)
	var exists bool
	err := tx.QueryRow(query, value).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of record in %s for %s=%s: %w", dbInfo.TableName, columnName, value, err)
	}
	return exists, nil
}

func prepareInsertStatement(tx *sql.Tx, dbInfo database.DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for i, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		dbInfo.TableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	// TODO: Add UPSERT (ON CONFLICT DO UPDATE) logic here

	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return stmt, nil
}

func convertToDBType(csvValue, dataType string, isNullable bool, columnDefault sql.NullString) (interface{}, error) {
	if csvValue == "" && isNullable {
		return nil, nil // Return nil for nullable empty strings
	}
	if csvValue == "" && columnDefault.Valid {
		csvValue = columnDefault.String // Use default value if CSV is empty and default exists
	}
	if csvValue == "" && !isNullable {
		// If not nullable and no default, provide a sensible default based on type
		switch dataType {
		case "text", "character varying", "varchar", "char":
			return "", nil
		case "integer", "smallint", "bigint", "numeric", "decimal", "real", "double precision":
			return 0, nil
		case "boolean":
			return false, nil
		case "date", "timestamp without time zone", "timestamp with time zone":
			return time.Time{}, nil // Or a specific zero value
		default:
			return nil, fmt.Errorf("non-nullable column with no default and empty CSV value for type %s", dataType)
		}
	}

	switch dataType {
	case "text", "character varying", "varchar", "char":
		return csvValue, nil
	case "integer", "smallint", "bigint":
		val, err := strconv.ParseInt(csvValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to integer: %w", csvValue, err)
		}
		return val, nil
	case "numeric", "decimal", "real", "double precision":
		val, err := strconv.ParseFloat(csvValue, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float: %w", csvValue, err)
		}
		return val, nil
	case "boolean":
		val, err := strconv.ParseBool(csvValue)
		if err != nil {
			// Try common string representations for boolean
			lowerVal := strings.ToLower(csvValue)
			if lowerVal == "true" || lowerVal == "t" || lowerVal == "1" || lowerVal == "yes" || lowerVal == "y" {
				return true, nil
			}
			if lowerVal == "false" || lowerVal == "f" || lowerVal == "0" || lowerVal == "no" || lowerVal == "n" {
				return false, nil
			}
			return nil, fmt.Errorf("failed to convert '%s' to boolean: %w", csvValue, err)
		}
		return val, nil
	case "date":
		// Assuming YYYY-MM-DD format
		val, err := time.Parse("2006-01-02", csvValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to date (expected YYYY-MM-DD): %w", csvValue, err)
		}
		return val, nil
	case "timestamp without time zone", "timestamp with time zone":
		// Assuming RFC3339 format (e.g., 2006-01-02T15:04:05Z07:00)
		val, err := time.Parse(time.RFC3339, csvValue)
		if err != nil {
			// Try other common formats if RFC3339 fails
			val, err = time.Parse("2006-01-02 15:04:05", csvValue)
			if err != nil {
				return nil, fmt.Errorf("failed to convert '%s' to timestamp: %w", csvValue, err)
			}
		}
		return val, nil
	default:
		// For unsupported types, return the string value and let the DB handle it,
		// or return an error if strict type checking is desired.
		log.Printf("Warning: Unsupported data type '%s' for value '%s'. Passing as string.\n", dataType, csvValue)
		return csvValue, nil
	}
}

func getCSVFiles(dir string) ([]string, error) {
	var csvFiles []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv") {
			csvFiles = append(csvFiles, filepath.Join(dir, entry.Name()))
		}
	}
	return csvFiles, nil
}
