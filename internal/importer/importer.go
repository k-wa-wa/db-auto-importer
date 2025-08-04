package importer

import (
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
// The 'hasHeader' parameter indicates whether all CSV files in the directory have a header row.
func (i *Importer) ImportCSVFiles(csvDir string, hasHeader bool) error {
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
		// Pass the hasHeader flag directly to ImportSingleCSV
		if err := i.ImportSingleCSV(filePath, dbInfo, hasHeader); err != nil {
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
		// If no header, assume CSV columns are in the same order as DB columns based on dbInfo.Columns order.
		// This creates a positional mapping from DB column name to its expected CSV index.
		for idx, colInfo := range dbInfo.Columns {
			columnMap[colInfo.ColumnName] = idx
		}
	}

	stmt, err := prepareInsertStatement(i.db, dbInfo)
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
			// Determine the CSV value based on the column mapping.
			// If the column is not found in the map (only possible if hasHeader is true and CSV is missing a DB column),
			// or if the mapped index is out of bounds for the current record (CSV has fewer columns than expected),
			// csvVal remains an empty string, which convertToDBType will handle as a missing/default value.
			if idx, ok := columnMap[colInfo.ColumnName]; ok && idx < len(record) {
				csvVal = record[idx]
			}

			// Check for foreign key constraints and ensure parent records exist
			for _, fk := range dbInfo.ForeignKeys {
				if fk.ColumnName == colInfo.ColumnName {
					parentDBInfo, ok := i.DBSchema[fk.ForeignTableName]
					if !ok {
						return fmt.Errorf("foreign table %s not found in schema info for foreign key %s", fk.ForeignTableName, fk.ConstraintName)
					}

					fkValue := csvVal
					// Skip foreign key check if the value is empty (null)
					if fkValue == "" {
						continue
					}

					err := i.ensureParentRecordExists(i.db, parentDBInfo, fk.ForeignColumnName, fkValue)
					if err != nil {
						return fmt.Errorf("failed to ensure parent record exists for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValue, err)
					}
					break
				}
			}

			convertedVal, err := convertToDBType(csvVal, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert value '%s' for column %s (%s) in table %s: %v. Skipping this value.\n", csvVal, colInfo.ColumnName, colInfo.DataType, dbInfo.TableName, err)
				// Depending on desired behavior, you might set convertedVal to nil or a default zero value
				// For now, we'll set it to nil, which will be handled by the database as NULL if the column is nullable.
				// If the column is NOT NULL, the database insert will likely fail, which is caught below.
				values[colIdx] = nil
			} else {
				values[colIdx] = convertedVal
			}
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			// Log the error and continue to the next record instead of returning
			log.Printf("Error inserting record into %s from file %s: %v. Record: %v\n", dbInfo.TableName, filePath, err, record)
			continue // Continue to the next record
		}
	}

	return nil
}

// ensureParentRecordExists checks if a record with the given foreignKeyValue exists in the parent table.
// If not, it creates a new record in the parent table with default values and the provided foreignKeyValue
// for the foreignColumnName.
func (i *Importer) ensureParentRecordExists(db *sql.DB, parentDBInfo database.DBInfo, foreignColumnName, foreignKeyValue string) error {
	// Check if the parent record already exists
	exists, err := i.parentRecordExists(db, parentDBInfo, foreignColumnName, foreignKeyValue)
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

	// First, populate parentValues with default/provided values
	for colIdx, colInfo := range parentDBInfo.Columns {
		parentCols = append(parentCols, colInfo.ColumnName)
		parentPlaceholders = append(parentPlaceholders, fmt.Sprintf("$%d", colIdx+1))

		var val interface{}
		if colInfo.ColumnName == foreignColumnName {
			// Use the foreignKeyValue for the foreign key column that triggered this call
			val, err = convertToDBType(foreignKeyValue, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert foreign key value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", foreignKeyValue, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Use nil if conversion fails
			}
		} else {
			// Use default values for other columns
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

				parentOfParentDBInfo, ok := i.DBSchema[fk.ForeignTableName]
				if !ok {
					return fmt.Errorf("foreign table %s not found in schema info for foreign key %s during recursive ensureParent", fk.ForeignTableName, fk.ConstraintName)
				}
				err := i.ensureParentRecordExists(db, parentOfParentDBInfo, fk.ForeignColumnName, fkValueStr)
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
	// TODO: Consider UPSERT for parent record creation if primary key might conflict

	_, err = db.Exec(insertQuery, parentValues...)
	if err != nil {
		return fmt.Errorf("failed to insert parent record into %s: %w", parentDBInfo.TableName, err)
	}

	return nil
}

// parentRecordExists checks if a record exists in the given table for a specific column and value.
func (i *Importer) parentRecordExists(db *sql.DB, dbInfo database.DBInfo, columnName, value string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = $1)", dbInfo.TableName, columnName)
	var exists bool
	err := db.QueryRow(query, value).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of record in %s for %s=%s: %w", dbInfo.TableName, columnName, value, err)
	}
	return exists, nil
}

func prepareInsertStatement(db *sql.DB, dbInfo database.DBInfo) (*sql.Stmt, error) {
	var cols []string
	var placeholders []string
	for i, colInfo := range dbInfo.Columns {
		cols = append(cols, colInfo.ColumnName)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	// Create a map for quick lookup of primary key columns
	pkMap := make(map[string]bool)
	for _, pkCol := range dbInfo.PrimaryKeyColumns {
		pkMap[pkCol] = true
	}

	var query string
	if len(dbInfo.PrimaryKeyColumns) > 0 {
		// Construct the ON CONFLICT DO UPDATE SET clause
		var updateClauses []string
		for _, colInfo := range dbInfo.Columns {
			// Do not update primary key columns in the SET clause, as they are used for conflict resolution
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
			// If there are no non-primary key columns to update, just do nothing on conflict
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
				dbInfo.TableName,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(dbInfo.PrimaryKeyColumns, ", "),
			)
		}
	} else {
		// No primary key defined, proceed with simple insert
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			dbInfo.TableName,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	}

	stmt, err := db.Prepare(query)
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
		case "text", "character varying", "varchar", "char", "character":
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
	case "text", "character varying", "varchar", "char", "character":
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
