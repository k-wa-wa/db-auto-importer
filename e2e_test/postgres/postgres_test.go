package e2e_test_postgres

import (
	"context"
	"database/sql"
	"db-auto-importer/internal/database"
	"log"
	"os"
	"testing"

	"db-auto-importer/e2e_test/common"
	"db-auto-importer/internal/app" // Import the new app package

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var dbConnStr string

func TestMain(m *testing.M) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("database"),
		postgres.WithUsername("username"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
		postgres.WithOrderedInitScripts(
			"../initdb.d/01-create-table.sql",
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	dbURL, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatal(err)
	}
	dbConnStr = dbURL + "sslmode=disable"

	os.Exit(m.Run())
}

func Test_schema情報を正しく読み取れること(t *testing.T) {
	t.Run("テーブル定義を正しく読み取れていること", func(t *testing.T) {
		dbClient, err := database.NewDBClient("postgres", dbConnStr)
		require.NoError(t, err)
		defer dbClient.Close()

		schemaInfo, err := dbClient.GetSchemaInfo("public")

		if diff := cmp.Diff(common.ExpectedDBInfo, schemaInfo); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})
}

func Test_csvを正しくimportできること(t *testing.T) {
	t.Run("importが成功すること", func(t *testing.T) {
		err := app.RunApp("postgres", dbConnStr, "../input_data/01", true, "public")
		require.NoError(t, err)
	})

	db, err := sql.Open("postgres", dbConnStr)
	require.NoError(t, err)
	defer db.Close()

	t.Run("postsが正しく作成されていること", func(t *testing.T) {
		expectedPosts := []common.Post{
			{ID: 1, Title: "First Post", UserID: 1},
			{ID: 2, Title: "Goについて", UserID: 1},
			{ID: 3, Title: "My Daily Life", UserID: 2},
			{ID: 4, Title: "AAA", UserID: 3},
		}

		var actualPosts []common.Post
		rows, err := db.Query("SELECT id, title, user_id FROM posts ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var p common.Post
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
		expectedUsers := []common.User{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
			{ID: 3, Name: ""}, // 自動で作成される
		}

		var actualUsers []common.User
		rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var u common.User
			err := rows.Scan(&u.ID, &u.Name)
			require.NoError(t, err)
			actualUsers = append(actualUsers, u)
		}
		require.NoError(t, rows.Err())

		if diff := cmp.Diff(expectedUsers, actualUsers); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("productsが正しく作成されていること", func(t *testing.T) {
		expectedProducts := []common.Product{
			{ID: 1, Name: "Laptop", Price: sql.NullFloat64{Valid: true, Float64: 1200.00}},
			{ID: 2, Name: "Mouse", Price: sql.NullFloat64{Valid: true, Float64: 25.50}},
			{ID: 3, Name: "---", Price: sql.NullFloat64{}}, // 自動で作成される。Nameはランダム生成されるため比較対象外とする
		}

		var actualProducts []common.Product
		rows, err := db.Query("SELECT id, name, price FROM products ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var p common.Product
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
		expectedTags := []common.Tag{
			{ID: 1, Name: "electronics"},
			{ID: 2, Name: "computer"},
			{ID: 3, Name: "---"}, // 自動で作成される。Nameはランダム生成されるため比較対象外とする
		}

		var actualTags []common.Tag
		rows, err := db.Query("SELECT id, name FROM tags ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var tag common.Tag
			err := rows.Scan(&tag.ID, &tag.Name)
			require.NoError(t, err)
			actualTags = append(actualTags, tag)
		}
		require.NoError(t, rows.Err())

		actualTags[2].Name = expectedTags[2].Name // 自動生成のためdiffが出ないようにする
		cmpOption := cmp.FilterValues(
			func(x, y interface{}) bool {
				tagX, okX := x.(common.Tag)
				return okX && tagX.Name == "---"
			},
			cmp.Ignore(),
		)
		if diff := cmp.Diff(expectedTags, actualTags, cmpOption); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})

	t.Run("product_tagsが正しく作成されていること", func(t *testing.T) {
		expectedProductTags := []common.ProductTag{
			{ProductID: 1, TagID: 1},
			{ProductID: 1, TagID: 2},
			{ProductID: 2, TagID: 1},
			{ProductID: 3, TagID: 3},
		}

		var actualProductTags []common.ProductTag
		rows, err := db.Query("SELECT product_id, tag_id FROM product_tags ORDER BY product_id, tag_id")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var pt common.ProductTag
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
