package main

import (
	"db-auto-importer/internal/database"
	"db-auto-importer/internal/importer"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	fmt.Println("db-auto-importer started.")

	// Define command-line flags
	dbType := flag.String("db-type", "postgres", "Database type (e.g., 'postgres', 'db2')")
	dbConnStr := flag.String("db", "postgresql://user:password@localhost:5432/dbname?sslmode=disable", "Database connection string")
	csvDir := flag.String("csv", "./testdata", "Directory containing CSV files")
	hasHeader := flag.Bool("header", true, "Set to false if CSV files do not have a header row")
	dbSchemaName := flag.String("schema", "public", "Database schema name to import into (e.g., 'public')")

	flag.Parse()

	// Initialize DBClient based on dbType
	dbClient, err := database.NewDBClient(*dbType, *dbConnStr)
	if err != nil {
		log.Fatalf("Error creating database client: %v", err)
	}
	defer dbClient.Close() // Ensure the database connection is closed

	// 1. Database Schema Detection
	schemaInfo, err := dbClient.GetSchemaInfo(*dbSchemaName)
	if err != nil {
		log.Fatalf("Error getting database schema info: %v", err)
	}
	fmt.Println("Database schema information retrieved successfully.")

	// 2. CSV Parsing and Data Import
	importer, err := importer.NewImporter(schemaInfo, dbClient)
	if err != nil {
		log.Fatalf("Error creating importer: %v", err)
	}
	// The importer now manages its own DBClient, so its Close method will call dbClient.Close
	// defer importer.Close() // No longer needed here, importer handles it

	// Pass the hasHeader flag to the importer
	if err := importer.ImportCSVFiles(*csvDir, *hasHeader); err != nil {
		log.Fatalf("Error importing CSV files: %v", err)
	}

	fmt.Println("db-auto-importer finished successfully.")
	os.Exit(0)
}
