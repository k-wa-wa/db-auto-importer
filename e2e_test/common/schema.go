package common

import (
	"database/sql"
	"db-auto-importer/internal/database"
)

var ExpectedDBInfo = map[string]database.DBInfo{
	"organizations": {
		TableName:         "organizations",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  nil,
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: database.StringType, IsNullable: false, ColumnDefault: sql.NullString{}},
		},
	},
	"users": {
		TableName:         "users",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  nil,
		ForeignKeys: []database.ForeignKeyInfo{
			{
				ConstraintName:    "fk_organization_id",
				TableName:         "users",
				ColumnName:        "organization_id",
				ForeignTableName:  "organizations",
				ForeignColumnName: "id",
			},
		},
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: database.StringType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "organization_id", DataType: database.IntegerType, IsNullable: true, ColumnDefault: sql.NullString{}},
			{ColumnName: "created_at", DataType: database.TimestampType, IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"posts": {
		TableName:         "posts",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  nil,
		ForeignKeys: []database.ForeignKeyInfo{
			{
				ConstraintName:    "fk_user_id",
				TableName:         "posts",
				ColumnName:        "user_id",
				ForeignTableName:  "users",
				ForeignColumnName: "id",
			},
		},
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "title", DataType: database.StringType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "content", DataType: database.StringType, IsNullable: true, ColumnDefault: sql.NullString{}},
			{ColumnName: "user_id", DataType: database.IntegerType, IsNullable: true, ColumnDefault: sql.NullString{}},
			{ColumnName: "published", DataType: database.BooleanType, IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"products": {
		TableName:         "products",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  [][]string{{"name"}},
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: database.StringType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "price", DataType: database.FloatType, IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"tags": {
		TableName:         "tags",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  [][]string{{"name"}},
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: database.StringType, IsNullable: false, ColumnDefault: sql.NullString{}},
		},
	},
	"product_tags": {
		TableName:         "product_tags",
		PrimaryKeyColumns: []string{"product_id", "tag_id"},
		UniqueKeyColumns:  nil,
		ForeignKeys: []database.ForeignKeyInfo{
			{
				ConstraintName:    "fk_product_id",
				TableName:         "product_tags",
				ColumnName:        "product_id",
				ForeignTableName:  "products",
				ForeignColumnName: "id",
			},
			{
				ConstraintName:    "fk_tag_id",
				TableName:         "product_tags",
				ColumnName:        "tag_id",
				ForeignTableName:  "tags",
				ForeignColumnName: "id",
			},
		},
		Columns: []database.ColumnInfo{
			{ColumnName: "product_id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "tag_id", DataType: database.IntegerType, IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "created_at", DataType: database.TimestampType, IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
}
