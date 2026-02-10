package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"dbx-explore/pkg/ui"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/spf13/cobra"
)

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "SQL-based exploration (System Tables)",
}

var listTablesCmd = &cobra.Command{
	Use:   "list-tables",
	Short: "List tables in system.information_schema",
	Run: func(cmd *cobra.Command, args []string) {
		ui.PrintInfo("Connecting to Databricks SQL...")

		dsn := os.Getenv("DATABRICKS_HOST")
		token := os.Getenv("DATABRICKS_TOKEN")
		httpPath := os.Getenv("DATABRICKS_HTTP_PATH")

		if dsn == "" || token == "" || httpPath == "" {
			ui.PrintError("Missing environment variables. Run 'dbx-explore auth login' first.")
			os.Exit(1)
		}

		// Remove https:// prefix if present logic might be needed or driver handles it?
		// Driver usually expects DSN in format: "databricks://<host>:443/default;httpPath=..."
		// Or we can register it with sql.Open("databricks", dsn)
		// But databricks-sql-go supports DSN construction.

		// Construct DSN
		// Example: "databricks://<host>:443/default?httpPath=<path>&accessToken=<token>"
		// Host usually should not have https://
		// Let's strip it.
		// Actually, let's just use the `databricks-sql-go` connector helper if possible or just format string.

		importPath := "github.com/databricks/databricks-sql-go"
		// Just to mention it.
		_ = importPath

		host := dsn
		if len(host) > 8 && host[:8] == "https://" {
			host = host[8:]
		}

		connStr := fmt.Sprintf("databricks://%s:443/default?httpPath=%s&accessToken=%s", host, httpPath, token)

		db, err := sql.Open("databricks", connStr)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Failed to open connection: %v", err))
			os.Exit(1)
		}
		defer db.Close()

		query := `
        SELECT table_catalog, table_schema, table_name 
        FROM system.information_schema.tables 
        WHERE table_catalog != 'system' 
        LIMIT 100
        `

		rows, err := db.Query(query)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Query failed: %v", err))
			os.Exit(1)
		}
		defer rows.Close()

		var result [][]string
		for rows.Next() {
			var catalog, schema, table string
			if err := rows.Scan(&catalog, &schema, &table); err != nil {
				ui.PrintError(fmt.Sprintf("Scan failed: %v", err))
				continue
			}
			result = append(result, []string{catalog, schema, table})
		}

		ui.PrintTable([]string{"Catalog", "Schema", "Table"}, result)
	},
}

func init() {
	rootCmd.AddCommand(sqlCmd)
	sqlCmd.AddCommand(listTablesCmd)
}
