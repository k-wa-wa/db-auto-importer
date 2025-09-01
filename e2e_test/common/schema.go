package common

import (
	"database/sql"
	"db-auto-importer/internal/database"
)

var ExpectedDBInfo = map[string]database.DBInfo{
	"users": database.DBInfo{
		TableName:         "users",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  nil,
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: "character varying", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "created_at", DataType: "timestamp without time zone", IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"posts": database.DBInfo{
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
			{ColumnName: "id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "title", DataType: "character varying", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "content", DataType: "text", IsNullable: true, ColumnDefault: sql.NullString{}},
			{ColumnName: "user_id", DataType: "integer", IsNullable: true, ColumnDefault: sql.NullString{}},
			{ColumnName: "published", DataType: "boolean", IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"products": database.DBInfo{
		TableName:         "products",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  [][]string{{"name"}},
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: "character varying", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "price", DataType: "numeric", IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
	"tags": database.DBInfo{
		TableName:         "tags",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  [][]string{{"name"}},
		ForeignKeys:       nil,
		Columns: []database.ColumnInfo{
			{ColumnName: "id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "name", DataType: "character varying", IsNullable: false, ColumnDefault: sql.NullString{}},
		},
	},
	"product_tags": database.DBInfo{
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
			{ColumnName: "product_id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "tag_id", DataType: "integer", IsNullable: false, ColumnDefault: sql.NullString{}},
			{ColumnName: "created_at", DataType: "timestamp without time zone", IsNullable: true, ColumnDefault: sql.NullString{}},
		},
	},
}
