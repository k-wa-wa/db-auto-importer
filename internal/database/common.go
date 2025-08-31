package database

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"crypto/rand"
	"strconv"
	"strings"
	"time"
)

// convertToDBType converts a CSV string value to the appropriate Go type for database insertion.
func convertToDBType(csvValue, dataType string, isNullable bool, columnDefault sql.NullString) (interface{}, error) {
	if csvValue == "" && isNullable {
		return nil, nil // Return nil for nullable empty strings
	}
	if csvValue == "" && columnDefault.Valid {
		csvValue = columnDefault.String // Use default value if CSV is empty and default exists
	}
	if csvValue == "" && !isNullable {
		// If not nullable and no default, provide a sensible default based on type.
		// This part is now handled by generateRandomValue if it's a unique key.
		// If not a unique key, we still need a default.
		switch dataType {
		case "text", "character varying", "varchar", "char", "character", "CLOB", "VARCHAR": // Added DB2 types
			return "", nil
		case "integer", "smallint", "bigint", "INT", "SMALLINT", "BIGINT": // Added DB2 types
			return 0, nil
		case "numeric", "decimal", "real", "double precision", "DECIMAL", "REAL", "DOUBLE": // Added DB2 types
			return 0.0, nil
		case "boolean", "BOOLEAN": // Added DB2 types
			return false, nil
		case "date", "timestamp without time zone", "timestamp with time zone", "DATE", "TIMESTAMP": // Added DB2 types
			return time.Time{}, nil // Zero value for time
		default:
			return nil, fmt.Errorf("non-nullable column with no default and empty CSV value for type %s", dataType)
		}
	}

	switch dataType {
	case "text", "character varying", "varchar", "char", "character", "CLOB", "VARCHAR":
		return csvValue, nil
	case "integer", "smallint", "bigint", "INT", "SMALLINT", "BIGINT":
		val, err := strconv.ParseInt(csvValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to integer: %w", csvValue, err)
		}
		return val, nil
	case "numeric", "decimal", "real", "double precision", "DECIMAL", "REAL", "DOUBLE":
		val, err := strconv.ParseFloat(csvValue, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float: %w", csvValue, err)
		}
		return val, nil
	case "boolean", "BOOLEAN":
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
	case "date", "DATE":
		// Assuming YYYY-MM-DD format
		val, err := time.Parse("2006-01-02", csvValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to date (expected YYYY-MM-DD): %w", csvValue, err)
		}
		return val, nil
	case "timestamp without time zone", "timestamp with time zone", "TIMESTAMP":
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

// generateRandomValue generates a random value suitable for database insertion based on data type.
// This is used for unique columns (PK/UK) that don't have a default value and are not the FK being inserted.
func generateRandomValue(dataType string) (interface{}, error) {
	switch dataType {
	case "text", "character varying", "varchar", "char", "character", "CLOB", "VARCHAR":
		b := make([]byte, 16) // 16 bytes for a 32-char hex string
		_, err := rand.Read(b)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random bytes for string: %w", err)
		}
		return hex.EncodeToString(b), nil
	case "integer", "smallint", "bigint", "INT", "SMALLINT", "BIGINT":
		// Generate a random int64
		max := big.NewInt(int64(^uint64(0) >> 1)) // Max int64
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random integer: %w", err)
		}
		return n.Int64(), nil
	case "numeric", "decimal", "real", "double precision", "DECIMAL", "REAL", "DOUBLE":
		// Generate a random float64 between 0 and 1, then scale it
		// This is a simple approach; for true randomness or specific ranges, more complex logic might be needed.
		max := big.NewInt(1e9) // For a reasonable range of floats
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random float: %w", err)
		}
		return float64(n.Int64()) / float64(max.Int64()), nil
	case "boolean", "BOOLEAN":
		// Random boolean
		b := make([]byte, 1)
		_, err := rand.Read(b)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random boolean: %w", err)
		}
		return b[0]%2 == 0, nil
	case "date", "timestamp without time zone", "timestamp with time zone", "DATE", "TIMESTAMP":
		// Generate a random time within a reasonable range (e.g., last 10 years)
		now := time.Now()
		tenYearsAgo := now.AddDate(-10, 0, 0)
		diff := now.Sub(tenYearsAgo)
		randomSeconds := big.NewInt(0)
		if diff.Seconds() > 0 {
			maxSeconds := big.NewInt(int64(diff.Seconds()))
			var err error
			randomSeconds, err = rand.Int(rand.Reader, maxSeconds)
			if err != nil {
				return nil, fmt.Errorf("failed to generate random time: %w", err)
			}
		}
		return tenYearsAgo.Add(time.Duration(randomSeconds.Int64()) * time.Second), nil
	default:
		return nil, fmt.Errorf("unsupported data type for random value generation: %s", dataType)
	}
}
