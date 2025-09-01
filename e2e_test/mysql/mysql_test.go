package e2e_test_mysql

import (
	"context"
	"database/sql"
	"db-auto-importer/internal/database"
	"log"
	"os"
	"testing"

	"db-auto-importer/e2e_test/common"
	"db-auto-importer/internal/app"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

var dbConnStr string

func TestMain(m *testing.M) {
	ctx := context.Background()

	mysqlContainer, err := mysql.Run(
		ctx,
		"mysql:8",
		mysql.WithDatabase("database"),
		mysql.WithUsername("username"),
		mysql.WithPassword("password"),
		mysql.WithScripts(
			"../initdb.d/01-create-table.sql",
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	dbURL, err := mysqlContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatal(err)
	}
	dbConnStr = dbURL

	os.Exit(m.Run())
}

func Test_schema情報を正しく読み取れること(t *testing.T) {
	t.Run("テーブル定義を正しく読み取れていること", func(t *testing.T) {
		dbClient, err := database.NewDBClient("mysql", dbConnStr)
		require.NoError(t, err)
		defer dbClient.Close()

		schemaInfo, err := dbClient.GetSchemaInfo("database") // MySQL uses database name as schema

		if diff := cmp.Diff(common.ExpectedDBInfo, schemaInfo); diff != "" {
			t.Errorf("diff: -want, +got:\n%s", diff)
		}
	})
}

func Test_csvを正しくimportできること(t *testing.T) {
	t.Run("importが成功すること", func(t *testing.T) {
		err := app.RunApp("mysql", dbConnStr, "../input_data/01", true, "database") // MySQL uses database name as schema
		require.NoError(t, err)
	})

	db, err := sql.Open("mysql", dbConnStr)
	require.NoError(t, err)
	defer db.Close()

	common.AssertAllDataCreated(t, db)
}
