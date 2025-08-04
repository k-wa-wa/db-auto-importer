package main

import (
	"db-auto-importer/internal/database"
	"db-auto-importer/internal/importer"
	"fmt"
	"log"
	"os"
)

func main() {
	fmt.Println("db-auto-importer started.")

	// TODO: Get database connection string and CSV directory from CLI arguments or environment variables.
	// For now, use placeholders.
	dbConnStr := "postgresql://user:password@localhost:5432/dbname?sslmode=disable" // Replace with your DB connection string
	csvDir := "./csv_files"                                                         // Replace with your CSV files directory

	// 1. Database Schema Detection
	schemaInfo, err := database.GetSchemaInfo(dbConnStr)
	if err != nil {
		log.Fatalf("Error getting database schema info: %v", err)
	}
	fmt.Println("Database schema information retrieved successfully.")

	// 2. CSV Parsing and Data Import
	importer, err := importer.NewImporter(schemaInfo, dbConnStr)
	if err != nil {
		log.Fatalf("Error creating importer: %v", err)
	}
	defer importer.Close() // Ensure the database connection is closed

	if err := importer.ImportCSVFiles(csvDir); err != nil {
		log.Fatalf("Error importing CSV files: %v", err)
	}

	fmt.Println("db-auto-importer finished successfully.")
	os.Exit(0)
}
