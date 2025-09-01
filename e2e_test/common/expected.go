package common

import (
	"database/sql"
	"db-auto-importer/internal/database"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
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

func AssertAllDataCreated(t *testing.T, db *sql.DB) {
	t.Helper()

	t.Run("postsが正しく作成されていること", func(t *testing.T) {
		expectedPosts := []Post{
			{ID: 1, Title: "First Post", UserID: 1},
			{ID: 2, Title: "Goについて", UserID: 1},
			{ID: 3, Title: "My Daily Life", UserID: 2},
			{ID: 4, Title: "AAA", UserID: 3},
		}

		var actualPosts []Post
		rows, err := db.Query("SELECT id, title, user_id FROM posts ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var p Post
			err := rows.Scan(&p.ID, &p.Title, &p.UserID)
			require.NoError(t, err)
			actualPosts = append(actualPosts, p)
		}
		require.NoError(t, rows.Err())

		if diff := cmp.Diff(expectedPosts, actualPosts); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("userが正しく作成されていること", func(t *testing.T) {
		expectedUsers := []User{
			{ID: 1, Name: "Alice", OrganizationID: sql.NullInt16{Valid: true, Int16: 1}},
			{ID: 2, Name: "Bob", OrganizationID: sql.NullInt16{Valid: true, Int16: 2}},
			{ID: 3, Name: ""}, // 自動で作成される
		}

		var actualUsers []User
		rows, err := db.Query("SELECT id, name, organization_id FROM users ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var u User
			err := rows.Scan(&u.ID, &u.Name, &u.OrganizationID)
			require.NoError(t, err)
			actualUsers = append(actualUsers, u)
		}
		require.NoError(t, rows.Err())

		if diff := cmp.Diff(expectedUsers, actualUsers); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("organizationsが正しく作成されていること", func(t *testing.T) {
		expectedOrganizations := []Organization{
			{ID: 1, Name: "Organization A"},
			{ID: 2, Name: ""},
		}

		var actualOrganizations []Organization
		rows, err := db.Query("SELECT id, name FROM organizations ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var o Organization
			err := rows.Scan(&o.ID, &o.Name)
			require.NoError(t, err)
			actualOrganizations = append(actualOrganizations, o)
		}
		require.NoError(t, rows.Err())

		if diff := cmp.Diff(expectedOrganizations, actualOrganizations); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("productsが正しく作成されていること", func(t *testing.T) {
		expectedProducts := []Product{
			{ID: 1, Name: "Laptop", Price: sql.NullFloat64{Valid: true, Float64: 1200.00}},
			{ID: 2, Name: "Mouse", Price: sql.NullFloat64{Valid: true, Float64: 25.50}},
			{ID: 3, Name: "---", Price: sql.NullFloat64{}}, // 自動で作成される。Nameはランダム生成されるため比較対象外とする
		}

		var actualProducts []Product
		rows, err := db.Query("SELECT id, name, price FROM products ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var p Product
			err := rows.Scan(&p.ID, &p.Name, &p.Price)
			require.NoError(t, err)
			actualProducts = append(actualProducts, p)
		}
		require.NoError(t, rows.Err())

		actualProducts[2].Name = expectedProducts[2].Name // 自動生成のためdiffが出ないようにする
		if diff := cmp.Diff(expectedProducts, actualProducts); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("tagsが正しく作成されていること", func(t *testing.T) {
		expectedTags := []Tag{
			{ID: 1, Name: "electronics"},
			{ID: 2, Name: "computer"},
			{ID: 3, Name: "---"}, // 自動で作成される。Nameはランダム生成されるため比較対象外とする
		}

		var actualTags []Tag
		rows, err := db.Query("SELECT id, name FROM tags ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var tag Tag
			err := rows.Scan(&tag.ID, &tag.Name)
			require.NoError(t, err)
			actualTags = append(actualTags, tag)
		}
		require.NoError(t, rows.Err())

		actualTags[2].Name = expectedTags[2].Name // 自動生成のためdiffが出ないようにする
		cmpOption := cmp.FilterValues(
			func(x, y interface{}) bool {
				tagX, okX := x.(Tag)
				return okX && tagX.Name == "---"
			},
			cmp.Ignore(),
		)
		if diff := cmp.Diff(expectedTags, actualTags, cmpOption); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("product_tagsが正しく作成されていること", func(t *testing.T) {
		expectedProductTags := []ProductTag{
			{ProductID: 1, TagID: 1},
			{ProductID: 1, TagID: 2},
			{ProductID: 2, TagID: 1},
			{ProductID: 3, TagID: 3},
		}

		var actualProductTags []ProductTag
		rows, err := db.Query("SELECT product_id, tag_id FROM product_tags ORDER BY product_id, tag_id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var pt ProductTag
			err := rows.Scan(&pt.ProductID, &pt.TagID)
			require.NoError(t, err)
			actualProductTags = append(actualProductTags, pt)
		}
		require.NoError(t, rows.Err())

		if diff := cmp.Diff(expectedProductTags, actualProductTags); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})
}
