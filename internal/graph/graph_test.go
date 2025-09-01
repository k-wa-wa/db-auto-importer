package graph

import (
	"db-auto-importer/e2e_test/common"
	"db-auto-importer/internal/database"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_TopologicalSort(t *testing.T) {
	t.Run("期待通りに並び替えられること・冪等性のある結果となること", func(t *testing.T) {
		expected := []string{"products", "tags", "users", "product_tags", "posts"}

		for i := 0; i < 10; i++ {
			sorted, err := NewGraph(common.ExpectedDBInfo).TopologicalSort()
			assert.NoError(t, err)
			require.Equal(t, expected, sorted)
		}
	})

	t.Run("循環参照がある場合にエラーを返すこと", func(t *testing.T) {
		schemaInfo := map[string]database.DBInfo{
			"tableA": {
				TableName: "tableA",
				ForeignKeys: []database.ForeignKeyInfo{
					{TableName: "tableA", ForeignTableName: "tableB"},
				},
			},
			"tableB": {
				TableName: "tableB",
				ForeignKeys: []database.ForeignKeyInfo{
					{TableName: "tableB", ForeignTableName: "tableC"},
				},
			},
			"tableC": {
				TableName: "tableC",
				ForeignKeys: []database.ForeignKeyInfo{
					{TableName: "tableC", ForeignTableName: "tableA"},
				},
			},
		}

		graph := NewGraph(schemaInfo)
		_, err := graph.TopologicalSort()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cycle detected", "Should detect a cycle and return an error")
	})
}
