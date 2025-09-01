package database

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// ColumnDataType represents a standardized database column type.
type ColumnDataType int

const (
	UnknownType ColumnDataType = iota
	StringType
	IntegerType
	FloatType
	BooleanType
	DateType
	TimestampType
	// Add other types as needed
)

func (cdt ColumnDataType) String() string {
	switch cdt {
	case StringType:
		return "STRING"
	case IntegerType:
		return "INTEGER"
	case FloatType:
		return "FLOAT"
	case BooleanType:
		return "BOOLEAN"
	case DateType:
		return "DATE"
	case TimestampType:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

// DBInfo holds information about a database table and its columns.
type DBInfo struct {
	TableName         string
	Columns           []ColumnInfo
	PrimaryKeyColumns []string
	UniqueKeyColumns  [][]string
	ForeignKeys       []ForeignKeyInfo
}

// ColumnInfo holds information about a database column.
type ColumnInfo struct {
	ColumnName    string
	DataType      ColumnDataType
	IsNullable    bool
	ColumnDefault sql.NullString
}

// ForeignKeyInfo holds information about a foreign key constraint.
type ForeignKeyInfo struct {
	ConstraintName    string
	TableName         string
	ColumnName        string
	ForeignTableName  string
	ForeignColumnName string
}

// ParseDataType converts a database-specific data type string to a standardized ColumnDataType.
func ParseDataType(dbType string) ColumnDataType {
	lowerDbType := strings.ToLower(dbType)
	switch lowerDbType {
	case "text", "character varying", "varchar", "char", "character", "clob", "graphic", "vargraphic", "long vargraphic":
		return StringType
	case "integer", "smallint", "bigint", "int":
		return IntegerType
	case "numeric", "decimal", "real", "double precision", "double", "decfloat", "float":
		return FloatType
	case "boolean", "bool":
		return BooleanType
	case "date":
		return DateType
	case "timestamp without time zone", "timestamp with time zone", "timestamp", "time":
		return TimestampType
	default:
		log.Printf("Warning: Unknown database data type '%s'. Mapping to UnknownType.\n", dbType)
		return UnknownType
	}
}

// ConvertToDBType converts a CSV string value to the appropriate Go type for database insertion.
func ConvertToDBType(csvValue string, dataType ColumnDataType, isNullable bool, columnDefault sql.NullString) (interface{}, error) {
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
		case StringType:
			return "", nil
		case IntegerType:
			return 0, nil
		case FloatType:
			return 0.0, nil
		case BooleanType:
			return false, nil
		case DateType, TimestampType:
			return time.Time{}, nil // Zero value for time
		default:
			return nil, fmt.Errorf("non-nullable column with no default and empty CSV value for type %s", dataType.String())
		}
	}

	switch dataType {
	case StringType:
		return csvValue, nil
	case IntegerType:
		val, err := strconv.ParseInt(csvValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to integer: %w", csvValue, err)
		}
		return val, nil
	case FloatType:
		val, err := strconv.ParseFloat(csvValue, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to float: %w", csvValue, err)
		}
		return val, nil
	case BooleanType:
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
	case DateType:
		// Assuming YYYY-MM-DD format
		val, err := time.Parse("2006-01-02", csvValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert '%s' to date (expected YYYY-MM-DD): %w", csvValue, err)
		}
		return val, nil
	case TimestampType:
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
		// For unsupported types, return an error as we now have a strict enum
		return nil, fmt.Errorf("unsupported data type '%s' for value '%s'", dataType.String(), csvValue)
	}
}

// ensureParentRecordExistsCommon contains the common logic for ensuring parent records.
// It handles value generation and recursive calls, but delegates database-specific
// operations (like checking existence and actual insertion) to the DBClient.
func ensureParentRecordExistsCommon(
	client DBClient,
	parentDBInfo DBInfo,
	foreignColumnName, foreignKeyValue string,
	dbSchema map[string]DBInfo,
) ([]string, []string, []interface{}, error) {
	// Prepare values for the new parent record
	parentCols := make([]string, 0, len(parentDBInfo.Columns))
	parentPlaceholders := make([]string, 0, len(parentDBInfo.Columns))
	parentValues := make([]interface{}, len(parentDBInfo.Columns))

	// Create a map for quick lookup of unique key columns (including primary keys)
	uniqueColsMap := make(map[string]bool)
	for _, pkCol := range parentDBInfo.PrimaryKeyColumns {
		uniqueColsMap[pkCol] = true
	}
	for _, ukCols := range parentDBInfo.UniqueKeyColumns {
		if len(ukCols) == 1 { // Only consider single-column unique constraints for random generation
			uniqueColsMap[ukCols[0]] = true
		}
	}

	// First, populate parentValues with default/provided/random values
	for colIdx, colInfo := range parentDBInfo.Columns {
		parentCols = append(parentCols, colInfo.ColumnName)
		// Placeholder will be database-specific, so we'll return these and let the caller format
		parentPlaceholders = append(parentPlaceholders, "") // Placeholder for now

		var val interface{}
		var err error

		if colInfo.ColumnName == foreignColumnName {
			// Use the foreignKeyValue for the foreign key column that triggered this call
			val, err = ConvertToDBType(foreignKeyValue, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert foreign key value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", foreignKeyValue, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Use nil if conversion fails
			}
		} else if colInfo.ColumnDefault.Valid {
			// Use the explicit column default if available
			val, err = ConvertToDBType(colInfo.ColumnDefault.String, colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
			if err != nil {
				log.Printf("Warning: Failed to convert default value '%s' for column %s (%s) in parent table %s: %v. Using nil.\n", colInfo.ColumnDefault.String, colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil
			}
		} else if uniqueColsMap[colInfo.ColumnName] && !colInfo.IsNullable {
			// If it's a unique column (PK or UK) and not nullable, generate a random value
			val, err = generateRandomValue(colInfo.DataType)
			if err != nil {
				log.Printf("Warning: Failed to generate random value for unique column %s (%s) in parent table %s: %v. Using nil.\n", colInfo.ColumnName, colInfo.DataType, parentDBInfo.TableName, err)
				val = nil // Fallback to nil if random generation fails
			}
		} else {
			// For other columns, use default behavior (empty string for ConvertToDBType)
			val, err = ConvertToDBType("", colInfo.DataType, colInfo.IsNullable, colInfo.ColumnDefault)
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

				parentOfParentDBInfo, ok := dbSchema[fk.ForeignTableName]
				if !ok {
					return nil, nil, nil, fmt.Errorf("foreign table %s not found in schema info for foreign key %s during recursive ensureParent", fk.ForeignTableName, fk.ConstraintName)
				}
				err := client.EnsureParentRecordExists(parentOfParentDBInfo, fk.ForeignColumnName, fkValueStr, dbSchema)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to recursively ensure parent record for %s.%s (value: %s): %w", fk.ForeignTableName, fk.ForeignColumnName, fkValueStr, err)
				}
			}
		} else {
			log.Printf("Warning: Foreign key column '%s' not found in parentDBInfo.Columns for table '%s'. Cannot recursively ensure its parent.\n", fk.ColumnName, parentDBInfo.TableName)
		}
	}
	return parentCols, parentPlaceholders, parentValues, nil
}

// generateRandomValue generates a random value suitable for database insertion based on data type.
// This is used for unique columns (PK/UK) that don't have a default value and are not the FK being inserted.
func generateRandomValue(dataType ColumnDataType) (interface{}, error) {
	switch dataType {
	case StringType:
		b := make([]byte, 16) // 16 bytes for a 32-char hex string
		_, err := rand.Read(b)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random bytes for string: %w", err)
		}
		return hex.EncodeToString(b), nil
	case IntegerType:
		// Generate a random int64
		max := big.NewInt(int64(^uint64(0) >> 1)) // Max int64
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random integer: %w", err)
		}
		return n.Int64(), nil
	case FloatType:
		// Generate a random float64 between 0 and 1, then scale it
		// This is a simple approach; for true randomness or specific ranges, more complex logic might be needed.
		max := big.NewInt(1e9) // For a reasonable range of floats
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random float: %w", err)
		}
		return float64(n.Int64()) / float64(max.Int64()), nil
	case BooleanType:
		// Random boolean
		b := make([]byte, 1)
		_, err := rand.Read(b)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random boolean: %w", err)
		}
		return b[0]%2 == 0, nil
	case DateType, TimestampType:
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
		return nil, fmt.Errorf("unsupported data type for random value generation: %s", dataType.String())
	}
}
