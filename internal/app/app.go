package app

import (
	"db-auto-importer/internal/database"
	"db-auto-importer/internal/importer"
	"fmt"
)

func RunApp(dbType, dbConnStr, csvDir string, hasHeader bool, dbSchemaName string) error {
	// Initialize DBClient based on dbType
	dbClient, err := database.NewDBClient(dbType, dbConnStr)
	if err != nil {
		return fmt.Errorf("error creating database client: %w", err)
	}
	defer dbClient.Close() // Ensure the database connection is closed

	// 1. Database Schema Detection
	schemaInfo, err := dbClient.GetSchemaInfo(dbSchemaName)
	if err != nil {
		return fmt.Errorf("error getting database schema info: %w", err)
	}
	fmt.Println("Database schema information retrieved successfully.")

	// 2. CSV Parsing and Data Import
	importer, err := importer.NewImporter(schemaInfo, dbClient)
	if err != nil {
		return fmt.Errorf("error creating importer: %w", err)
	}
	// The importer now manages its own DBClient, so its Close method will call dbClient.Close
	// defer importer.Close() // No longer needed here, importer handles it

	// Pass the hasHeader flag to the importer
	if err := importer.ImportCSVFiles(csvDir, hasHeader); err != nil {
		return fmt.Errorf("error importing CSV files: %w", err)
	}

	return nil
}
