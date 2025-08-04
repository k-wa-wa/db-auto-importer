package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"db-auto-importer/internal/database"
	importerPackage "db-auto-importer/internal/importer"
)

const (
	dbHost     = "localhost"
	dbPort     = "5433" // Updated port
	dbUser     = "testuser"
	dbPassword = "testpassword"
	dbName     = "testdb"
)

func TestMain(m *testing.M) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	// Wait for the database to be ready
	err := waitForDB(connStr, 10*time.Second)
	if err != nil {
		log.Fatalf("Database not ready: %v", err)
	}

	// Run tests
	code := m.Run()

	// Teardown (optional, depending on whether you want to keep the Docker container running)
	// docker-compose down can be run manually after tests are complete.

	os.Exit(code)
}

func waitForDB(connStr string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Printf("Error opening database: %v. Retrying...", err)
			time.Sleep(1 * time.Second)
			continue
		}
		defer db.Close()

		err = db.Ping()
		if err == nil {
			log.Println("Database is ready!")
			return nil
		}
		log.Printf("Database not ready: %v. Retrying...", err)
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timed out waiting for database to be ready")
}

func createTestTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE
		);

		CREATE TABLE IF NOT EXISTS orders (
			order_id SERIAL PRIMARY KEY,
			user_id INT NOT NULL,
			product VARCHAR(100),
			quantity INT,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);

		CREATE TABLE IF NOT EXISTS products (
			product_id SERIAL PRIMARY KEY,
			product_name VARCHAR(100) UNIQUE NOT NULL,
			price DECIMAL(10, 2)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create test tables: %w", err)
	}
	log.Println("Test tables created successfully.")
	return nil
}

func TestGetSchemaInfo(t *testing.T) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	err = createTestTables(db)
	if err != nil {
		t.Fatalf("Failed to create test tables: %v", err)
	}

	schemaInfo, err := database.GetSchemaInfo(connStr, "public") // Assuming "public" schema for tests
	if err != nil {
		t.Fatalf("Failed to get schema info: %v", err)
	}

	// Assertions for users table
	usersInfo, ok := schemaInfo["users"]
	if !ok {
		t.Errorf("users table not found in schema info")
	} else {
		if usersInfo.TableName != "users" {
			t.Errorf("Expected table name 'users', got '%s'", usersInfo.TableName)
		}
		if len(usersInfo.Columns) != 3 {
			t.Errorf("Expected 3 columns for users table, got %d", len(usersInfo.Columns))
		}
		if len(usersInfo.PrimaryKeyColumns) != 1 || usersInfo.PrimaryKeyColumns[0] != "id" {
			t.Errorf("Expected primary key 'id' for users table, got %v", usersInfo.PrimaryKeyColumns)
		}
		if len(usersInfo.UniqueKeyColumns) != 1 || len(usersInfo.UniqueKeyColumns[0]) != 1 || usersInfo.UniqueKeyColumns[0][0] != "email" {
			t.Errorf("Expected unique key 'email' for users table, got %v", usersInfo.UniqueKeyColumns)
		}
		if len(usersInfo.ForeignKeys) != 0 {
			t.Errorf("Expected 0 foreign keys for users table, got %d", len(usersInfo.ForeignKeys))
		}
	}

	// Assertions for orders table
	ordersInfo, ok := schemaInfo["orders"]
	if !ok {
		t.Errorf("orders table not found in schema info")
	} else {
		if ordersInfo.TableName != "orders" {
			t.Errorf("Expected table name 'orders', got '%s'", ordersInfo.TableName)
		}
		if len(ordersInfo.Columns) != 4 {
			t.Errorf("Expected 4 columns for orders table, got %d", len(ordersInfo.Columns))
		}
		if len(ordersInfo.PrimaryKeyColumns) != 1 || ordersInfo.PrimaryKeyColumns[0] != "order_id" {
			t.Errorf("Expected primary key 'order_id' for orders table, got %v", ordersInfo.PrimaryKeyColumns)
		}
		if len(ordersInfo.UniqueKeyColumns) != 0 {
			t.Errorf("Expected 0 unique keys for orders table, got %d", len(ordersInfo.UniqueKeyColumns))
		}
		if len(ordersInfo.ForeignKeys) != 1 || ordersInfo.ForeignKeys[0].ColumnName != "user_id" || ordersInfo.ForeignKeys[0].ForeignTableName != "users" || ordersInfo.ForeignKeys[0].ForeignColumnName != "id" {
			t.Errorf("Expected foreign key on user_id referencing users(id) for orders table, got %v", ordersInfo.ForeignKeys)
		}
	}

	// Assertions for products table
	productsInfo, ok := schemaInfo["products"]
	if !ok {
		t.Errorf("products table not found in schema info")
	} else {
		if productsInfo.TableName != "products" {
			t.Errorf("Expected table name 'products', got '%s'", productsInfo.TableName)
		}
		if len(productsInfo.Columns) != 3 {
			t.Errorf("Expected 3 columns for products table, got %d", len(productsInfo.Columns))
		}
		if len(productsInfo.PrimaryKeyColumns) != 1 || productsInfo.PrimaryKeyColumns[0] != "product_id" {
			t.Errorf("Expected primary key 'product_id' for products table, got %v", productsInfo.PrimaryKeyColumns)
		}
		if len(productsInfo.UniqueKeyColumns) != 1 || len(productsInfo.UniqueKeyColumns[0]) != 1 || productsInfo.UniqueKeyColumns[0][0] != "product_name" {
			t.Errorf("Expected unique key 'product_name' for products table, got %v", productsInfo.UniqueKeyColumns)
		}
		if len(productsInfo.ForeignKeys) != 0 {
			t.Errorf("Expected 0 foreign keys for products table, got %d", len(productsInfo.ForeignKeys))
		}
	}
}

func TestImportCSVFiles(t *testing.T) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Clean up tables before import test
	_, err = db.Exec(`
		DROP TABLE IF EXISTS orders CASCADE;
		DROP TABLE IF EXISTS users CASCADE;
		DROP TABLE IF EXISTS products CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to clean up tables: %v", err)
	}

	err = createTestTables(db)
	if err != nil {
		t.Fatalf("Failed to create test tables: %v", err)
	}

	schemaInfo, err := database.GetSchemaInfo(connStr, "public") // Assuming "public" schema for tests
	if err != nil {
		t.Fatalf("Failed to get schema info: %v", err)
	}

	importer, err := importerPackage.NewImporter(schemaInfo, connStr)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	csvDir := "testdata"
	err = importer.ImportCSVFiles(csvDir, true) // Assuming CSVs in testdata generally have headers for this test
	if err != nil {
		t.Fatalf("Failed to import CSV files: %v", err)
	}

	// Verify data in users table
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("Failed to query users count: %v", err)
	}
	if userCount != 3 {
		t.Errorf("Expected 3 users, got %d", userCount)
	}

	// Verify data in products table
	var productCount int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&productCount)
	if err != nil {
		t.Fatalf("Failed to query products count: %v", err)
	}
	if productCount != 3 {
		t.Errorf("Expected 3 products, got %d", productCount)
	}

	// Verify data in orders table
	var orderCount int
	err = db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&orderCount)
	if err != nil {
		t.Fatalf("Failed to query orders count: %v", err)
	}
	if orderCount != 4 {
		t.Errorf("Expected 4 orders, got %d", orderCount)
	}

	// Verify a specific record in orders table
	var userId int
	var product string
	err = db.QueryRow("SELECT user_id, product FROM orders WHERE order_id = 1").Scan(&userId, &product)
	if err != nil {
		t.Fatalf("Failed to query order 1: %v", err)
	}
	if userId != 1 || product != "Laptop" {
		t.Errorf("Expected order 1 to be user_id 1 and product Laptop, got user_id %d and product %s", userId, product)
	}
}

func TestImportCSVFilesNoHeader(t *testing.T) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Clean up tables before import test
	_, err = db.Exec(`
		DROP TABLE IF EXISTS users CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to clean up tables: %v", err)
	}

	// Create only the users table for this test
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}
	log.Println("Users table created successfully for no-header test.")

	schemaInfo, err := database.GetSchemaInfo(connStr, "public") // Assuming "public" schema for tests
	if err != nil {
		t.Fatalf("Failed to get schema info: %v", err)
	}

	importerInstance, err := importerPackage.NewImporter(schemaInfo, connStr)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importerInstance.Close()

	// Manually import the no-header CSV, specifying hasHeader as false
	filePath := "testdata/users_no_header.csv"
	dbInfo, ok := schemaInfo["users"]
	if !ok {
		t.Fatalf("users table not found in schema info for no-header test")
	}

	fmt.Printf("Importing data from %s into table %s (no header)...\n", filePath, dbInfo.TableName)
	err = importerInstance.ImportSingleCSV(filePath, dbInfo, false) // Explicitly set hasHeader to false
	if err != nil {
		t.Fatalf("Failed to import no-header CSV file: %v", err)
	}
	fmt.Printf("Finished importing %s (no header).\n", filePath)

	// Verify data in users table
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("Failed to query users count after no-header import: %v", err)
	}
	if userCount != 2 {
		t.Errorf("Expected 2 users from no-header CSV, got %d", userCount)
	}

	var name string
	var email string
	err = db.QueryRow("SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	if err != nil {
		t.Fatalf("Failed to query user 1 from no-header import: %v", err)
	}
	if name != "AliceNoHeader" || email != "alice_no_header@example.com" {
		t.Errorf("Expected user 1 to be AliceNoHeader/alice_no_header@example.com, got %s/%s", name, email)
	}
}
