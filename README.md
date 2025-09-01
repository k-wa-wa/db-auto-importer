# db-auto-importer

`db-auto-importer` は、CSVファイル群をリレーショナルデータベースに自動的にインポートするツールである。外部キー制約を考慮し、必要に応じて親テーブルのレコードを自動生成することで、データの整合性を保ちながらインポートを行う。

## 使い方

### コマンドライン引数

`db-auto-importer` は以下のコマンドライン引数をサポートする。

*   `--db-type`: データベースの種類を指定する (例: `postgres`, `mysql`, `db2`)。デフォルトは `postgres` である。
*   `--db`: データベース接続文字列を指定する (例: `postgresql://user:password@localhost:5432/dbname?sslmode=disable`)。
*   `--csv`: CSVファイルが格納されているディレクトリのパスを指定する (例: `./testdata`)。
*   `--header`: CSVファイルにヘッダー行があるかどうかを指定する (`true` または `false`)。デフォルトは `true` である。
*   `--schema`: インポート先のデータベーススキーマ名を指定する (例: `public`)。デフォルトは `public` である。

### 実行例

```bash
go build .

./db-auto-importer \
    --db-type postgres \
    --db "postgresql://user:password@localhost:5432/dbname?sslmode=disable" \
    --csv "./path/to/csvs" \
    --header true \
    --schema public
```

※ DB2 で使用する場合は以下のビルドコマンドを使用する

```bash
go build -tags ibm_db .
```

### テストの実行

このプロジェクトには、PostgreSQL と MySQL の両方に対する E2E テストが含まれる。テストを実行するには、Docker と Go がインストールされている必要がある。

```bash
# MySQL のテストを実行
go test ./e2e_test/mysql -v

# PostgreSQL のテストを実行
go test ./e2e_test/postgres -v
```
