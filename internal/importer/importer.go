package importer

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"db-auto-importer/internal/database"
	"db-auto-importer/internal/graph"
)

// Importer handles the CSV parsing and data import logic.
type Importer struct {
	DBSchema map[string]database.DBInfo
	DBClient database.DBClient // Use the DBClient interface
}

// NewImporter creates a new Importer instance.
func NewImporter(dbSchema map[string]database.DBInfo, dbClient database.DBClient) (*Importer, error) {
	return &Importer{
		DBSchema: dbSchema,
		DBClient: dbClient,
	}, nil
}

// Close closes the database connection.
func (i *Importer) Close() error {
	return i.DBClient.Close()
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

	stmt, err := i.DBClient.PrepareInsertStatement(dbInfo)
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
			}

			for _, fk := range dbInfo.ForeignKeys {
				if fk.ColumnName == colInfo.ColumnName {
					parentDBInfo, ok := i.DBSchema[fk.ForeignTableName]
					if !ok {
						return fmt.Errorf("foreign table %s not found in schema info for foreign key %s", fk.ForeignTableName, fk.ConstraintName)
					}

					fkValue := csvVal
					if fkValue == "" {
						continue
					}

					err := i.DBClient.EnsureParentRecordExists(parentDBInfo, fk.ForeignColumnName, fkValue, i.DBSchema)
					if err != nil {
						return fmt.Errorf("failed to ensure parent record exists for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValue, err)
					}
					break
				}
			}

			convertedVal, err := database.ConvertToDBType(csvVal, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert value '%s' for column %s (%s) in table %s: %v. Skipping this value.\n", csvVal, colInfo.ColumnName, colInfo.DataType, dbInfo.TableName, err)
				values[colIdx] = nil
			} else {
				values[colIdx] = convertedVal
			}
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			log.Printf("Error inserting record into %s from file %s: %v. Record: %v\n", dbInfo.TableName, filePath, err, record)
			continue
		}
	}

	return nil
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
