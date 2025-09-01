package main

import (
	"db-auto-importer/internal/app" // Import the new app package
	"flag"
	"log"
	"os"
)

func main() {
	log.Println("db-auto-importer started.")

	// Define command-line flags
	dbType := flag.String("db-type", "postgres", "Database type (e.g., 'postgres', 'db2')")
	dbConnStr := flag.String("db", "postgresql://user:password@localhost:5432/dbname?sslmode=disable", "Database connection string")
	csvDir := flag.String("csv", "./testdata", "Directory containing CSV files")
	hasHeader := flag.Bool("header", true, "Set to false if CSV files do not have a header row")
	dbSchemaName := flag.String("schema", "public", "Database schema name to import into (e.g., 'public')")

	flag.Parse()
	if err := app.RunApp(*dbType, *dbConnStr, *csvDir, *hasHeader, *dbSchemaName); err != nil {
		log.Fatalf("Error running application: %v", err)
	}

	log.Println("db-auto-importer finished successfully.")
	os.Exit(0)
}
