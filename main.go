package main

import (
	"db-auto-importer/internal/database"
	"db-auto-importer/internal/importer"
	"flag" // Import the flag package
	"fmt"
	"log"
	"os"
)

func main() {
	fmt.Println("db-auto-importer started.")

	// Define command-line flags
	dbConnStr := flag.String("db", "postgresql://user:password@localhost:5432/dbname?sslmode=disable", "Database connection string")
	csvDir := flag.String("csv", "./testdata", "Directory containing CSV files")
	hasHeader := flag.Bool("header", true, "Set to false if CSV files do not have a header row")

	flag.Parse() // Parse the command-line arguments

	// 1. Database Schema Detection
	schemaInfo, err := database.GetSchemaInfo(*dbConnStr)
	if err != nil {
		log.Fatalf("Error getting database schema info: %v", err)
	}
	fmt.Println("Database schema information retrieved successfully.")

	// 2. CSV Parsing and Data Import
	importer, err := importer.NewImporter(schemaInfo, *dbConnStr)
	if err != nil {
		log.Fatalf("Error creating importer: %v", err)
	}
	defer importer.Close() // Ensure the database connection is closed

	// Pass the hasHeader flag to the importer
	if err := importer.ImportCSVFiles(*csvDir, *hasHeader); err != nil {
		log.Fatalf("Error importing CSV files: %v", err)
	}

	fmt.Println("db-auto-importer finished successfully.")
	os.Exit(0)
}
