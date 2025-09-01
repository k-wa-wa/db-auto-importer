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

	common.AssertAllDataCreated(t, db)
}
