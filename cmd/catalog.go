package cmd

import (
	"context"
	"fmt"
	"os"

	"dbx-explore/pkg/ui"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/spf13/cobra"
)

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "REST API-based exploration (Catalogs, Schemas, Tables)",
}

var listCatalogsCmd = &cobra.Command{
	Use:   "list-catalogs",
	Short: "List all catalogs",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		w := getWorkspaceClient()

		ui.PrintInfo("Listing catalogs...")
		iterator := w.Catalogs.List(ctx, catalog.ListCatalogsRequest{})

		var rows [][]string
		for iterator.HasNext(ctx) {
			c, err := iterator.Next(ctx)
			if err != nil {
				ui.PrintError(fmt.Sprintf("Error iterating catalogs: %v", err))
				break
			}
			rows = append(rows, []string{c.Name, fmt.Sprintf("%v", c.Owner), c.Comment})
		}

		ui.PrintTable([]string{"Name", "Owner", "Comment"}, rows)
	},
}

func init() {
	rootCmd.AddCommand(catalogCmd)
	catalogCmd.AddCommand(listCatalogsCmd)
}

func getWorkspaceClient() *databricks.WorkspaceClient {
	// SDK automatically loads from env vars DATABRICKS_HOST, DATABRICKS_TOKEN
	w, err := databricks.NewWorkspaceClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize Databricks Client: %v\n", err)
		os.Exit(1)
	}
	return w
}
