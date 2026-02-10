package cmd

import (
	"context"
	"fmt"
	"os"

	"dbx-explore/pkg/auth"
	"dbx-explore/pkg/ui"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	sql2 "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/spf13/cobra"
)

var interactiveCmd = &cobra.Command{
	Use:   "interactive",
	Short: "Interactive Wizard Mode",
	Run: func(cmd *cobra.Command, args []string) {
		startInteractiveJourney()
	},
}

func init() {
	rootCmd.AddCommand(interactiveCmd)
}

func startInteractiveJourney() {
	ui.PrintHeader("Interactive Journey")

	// 0. Start Screen Menu
	for {
		// Check if we have credentials
		hasCreds := os.Getenv("DATABRICKS_HOST") != "" && os.Getenv("DATABRICKS_TOKEN") != ""

		menuItems := []string{"🚀 Start Exploration"}
		if hasCreds {
			host := os.Getenv("DATABRICKS_HOST")
			ui.PrintInfo(fmt.Sprintf("Logged in as: %s", host))
			menuItems = append(menuItems, "🔄 Reset Credentials / Login")
			// Show current warehouse if selected, or option to select
			warehouseID := os.Getenv("DATABRICKS_WAREHOUSE_ID")
			if warehouseID != "" {
				menuItems = append(menuItems, "🏭 Switch SQL Warehouse")
			} else {
				menuItems = append(menuItems, "🏭 Select SQL Warehouse")
			}
		} else {
			ui.PrintInfo("Status: Not Logged In")
		}
		menuItems = append(menuItems, "❌ Exit")

		_, choice, err := ui.SelectPrompt("Main Menu", menuItems)
		if err != nil {
			return
		}

		if choice == "❌ Exit" {
			ui.PrintInfo("Bye! 👋")
			return
		}

		if choice == "🔄 Reset Credentials / Login" {
			if err := auth.ClearCredentials(); err != nil {
				ui.PrintError(fmt.Sprintf("Failed to clear credentials: %v", err))
			}
			// Trigger login immediately
			if err := auth.RunInteractiveLogin(); err != nil {
				ui.PrintError(fmt.Sprintf("Login failed: %v", err))
			}
			continue // Loop back to menu
		}

		if choice == "🏭 Switch SQL Warehouse" || choice == "🏭 Select SQL Warehouse" {
			selectWarehouse()
			continue
		}

		// Start Exploration
		// 0. Check Auth (Double check)
		if os.Getenv("DATABRICKS_HOST") == "" || os.Getenv("DATABRICKS_TOKEN") == "" {
			ui.PrintInfo("No credentials found. Starting interactive login...")
			if err := auth.RunInteractiveLogin(); err != nil {
				ui.PrintError(fmt.Sprintf("Login failed: %v", err))
				continue // Back to menu
			}
		}

		// Proceed to catalogs
		runCatalogLoop()
		// Loop back to main menu after returning from exploration
	}
}

func selectWarehouse() {
	ctx := context.Background()
	w := getWorkspaceClient()

	ui.PrintInfo("Fetching SQL Warehouses...")
	warehouses, err := auth.ListWarehouses(ctx, w)
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to list warehouses: %v", err))
		return
	}

	if len(warehouses) == 0 {
		ui.PrintError("No SQL Warehouses found.")
		return
	}

	var items []string
	var ids []string

	currentID := os.Getenv("DATABRICKS_WAREHOUSE_ID")

	for _, wh := range warehouses {
		state := wh.State
		icon := "❓"
		switch state {
		case sql2.StateRunning:
			icon = "✅"
		case sql2.StateStarting:
			icon = "⏳"
		case sql2.StateStopped:
			icon = "🛑"
		}

		prefix := "  "
		if wh.Id == currentID {
			prefix = "👉"
		}

		typeLabel := ""
		if wh.EnableServerlessCompute {
			typeLabel = "⚡ [Serverless]"
		}

		items = append(items, fmt.Sprintf("%s %s %s (%s) %s", prefix, icon, wh.Name, wh.ClusterSize, typeLabel))
		ids = append(ids, wh.Id)
	}

	items = append(items, "❌ Cancel")

	idx, choice, err := ui.SelectPrompt("Select SQL Warehouse", items)
	if err != nil || choice == "❌ Cancel" {
		return
	}

	selectedID := ids[idx]
	if err := auth.UpdateEnvWarehouse(selectedID); err != nil {
		ui.PrintError(fmt.Sprintf("Failed to update .env: %v", err))
		return
	}

	ui.PrintSuccess(fmt.Sprintf("Selected Warehouse ID: %s", selectedID))
}

func runCatalogLoop() {
	ctx := context.Background()
	w := getWorkspaceClient()

	// Loop for Catalog Selection
	for {
		// 1. Select Catalog
		ui.PrintInfo("Fetching Catalogs...")
		catIter := w.Catalogs.List(ctx, catalog.ListCatalogsRequest{})

		var catalogs []catalog.CatalogInfo
		for catIter.HasNext(ctx) {
			c, err := catIter.Next(ctx)
			if err != nil {
				ui.PrintError(fmt.Sprintf("Failed to list catalogs: %v", err))
				return
			}
			catalogs = append(catalogs, c)
		}

		if len(catalogs) == 0 {
			ui.PrintError("No catalogs found.")
			return
		}

		catalogNames := make([]string, len(catalogs)+1)
		for i, c := range catalogs {
			catalogNames[i] = c.Name
		}
		// Change "Exit" to "Back to Main Menu"
		catalogNames[len(catalogs)] = "⬅️  Back to Main Menu"

		_, selectedCatalog, err := ui.SelectPrompt("Select Catalog", catalogNames)
		if err != nil {
			return
		}
		if selectedCatalog == "⬅️  Back to Main Menu" {
			return
		}
		ui.PrintSuccess(fmt.Sprintf("Selected Catalog: %s", selectedCatalog))

		// Loop for Schema Selection
		navigateSchemas(ctx, w, selectedCatalog)
	}
}

func navigateSchemas(ctx context.Context, w *databricks.WorkspaceClient, catalogName string) {
	for {
		// 2. Select Schema
		ui.PrintInfo(fmt.Sprintf("Fetching Schemas in %s...", catalogName))
		schemaIter := w.Schemas.List(ctx, catalog.ListSchemasRequest{CatalogName: catalogName})

		var schemas []catalog.SchemaInfo
		for schemaIter.HasNext(ctx) {
			s, err := schemaIter.Next(ctx)
			if err != nil {
				ui.PrintError(fmt.Sprintf("Failed to list schemas: %v", err))
				return
			}
			schemas = append(schemas, s)
		}

		schemaNames := make([]string, len(schemas)+1)
		for i, s := range schemas {
			schemaNames[i] = s.Name
		}
		schemaNames[len(schemas)] = "⬅️  Back"

		_, selectedSchema, err := ui.SelectPrompt("Select Schema", schemaNames)
		if err != nil {
			return
		}
		if selectedSchema == "⬅️  Back" {
			return
		}
		ui.PrintSuccess(fmt.Sprintf("Selected Schema: %s", selectedSchema))

		// Loop for Table Selection
		navigateTables(ctx, w, catalogName, selectedSchema)
	}
}

func navigateTables(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) {
	for {
		// 3. Select Table
		ui.PrintInfo(fmt.Sprintf("Fetching Tables in %s.%s...", catalogName, schemaName))
		tableIter := w.Tables.List(ctx, catalog.ListTablesRequest{CatalogName: catalogName, SchemaName: schemaName})

		var tables []catalog.TableInfo
		for tableIter.HasNext(ctx) {
			t, err := tableIter.Next(ctx)
			if err != nil {
				ui.PrintError(fmt.Sprintf("Failed to list tables: %v", err))
				return
			}
			tables = append(tables, t)
		}

		tableNames := make([]string, len(tables)+1)
		for i, t := range tables {
			tableNames[i] = t.Name
		}
		tableNames[len(tables)] = "⬅️  Back"

		_, selectedTable, err := ui.SelectPrompt("Select Table", tableNames)
		if err != nil {
			return
		}
		if selectedTable == "⬅️  Back" {
			return
		}
		ui.PrintSuccess(fmt.Sprintf("Selected Table: %s", selectedTable))

		// 4. Action Menu
		navigateTableActions(ctx, w, catalogName, schemaName, selectedTable)
	}
}

func navigateTableActions(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName, tableName string) {
	for {
		ui.PrintHeader(fmt.Sprintf("Table: %s", tableName))

		actions := []string{
			"📋 View Columns",
			"ℹ️  Extended Metadata",
			"📊 Sample Data (Limit 5)",
			"⬅️  Back to Tables",
		}

		_, choice, err := ui.SelectPrompt("Choose Action", actions)
		if err != nil {
			return
		}

		switch choice {
		case "📋 View Columns":
			showColumns(ctx, w, catalogName, schemaName, tableName)
		case "ℹ️  Extended Metadata":
			showExtendedMetadata(ctx, w, catalogName, schemaName, tableName)
		case "📊 Sample Data (Limit 5)":
			sampleData(ctx, catalogName, schemaName, tableName)
		case "⬅️  Back to Tables":
			return
		}

		fmt.Println("\nPress Enter to continue...")
		fmt.Scanln()
	}
}

func showColumns(ctx context.Context, w *databricks.WorkspaceClient, c, s, t string) {
	tableInfo, err := w.Tables.Get(ctx, catalog.GetTableRequest{FullName: fmt.Sprintf("%s.%s.%s", c, s, t)})
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to get table details: %v", err))
		return
	}

	var rows [][]string
	for _, col := range tableInfo.Columns {
		comment := col.Comment
		if comment == "" {
			comment = "-"
		}
		rows = append(rows, []string{col.Name, fmt.Sprintf("%v", col.TypeName), comment})
	}
	ui.PrintTable([]string{"Column", "Type", "Comment"}, rows)
}

func showExtendedMetadata(ctx context.Context, w *databricks.WorkspaceClient, c, s, t string) {
	tableInfo, err := w.Tables.Get(ctx, catalog.GetTableRequest{FullName: fmt.Sprintf("%s.%s.%s", c, s, t)})
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to get table details: %v", err))
		return
	}

	data := map[string]string{
		"ID":               tableInfo.TableId,
		"Type":             fmt.Sprintf("%v", tableInfo.TableType),
		"Owner":            tableInfo.Owner,
		"Created By":       tableInfo.CreatedBy,
		"Storage Location": tableInfo.StorageLocation,
		"Format":           fmt.Sprintf("%v", tableInfo.DataSourceFormat),
	}

	ui.PrintKeyValue("Extended Metadata", data)
}

func sampleData(ctx context.Context, c, s, t string) {
	ui.PrintInfo("Querying data (via Statement Execution)...")

	warehouseID := os.Getenv("DATABRICKS_WAREHOUSE_ID")
	if warehouseID == "" {
		ui.PrintError("DATABRICKS_WAREHOUSE_ID is missing.")
		ui.PrintInfo("Prompting for selection...")
		selectWarehouse()
		// Retry fetch
		warehouseID = os.Getenv("DATABRICKS_WAREHOUSE_ID")
		if warehouseID == "" {
			ui.PrintError("No warehouse selected. Aborting query.")
			return
		}
	}

	w := getWorkspaceClient()

	// Fetch warehouse details to show status
	whInfo, err := w.Warehouses.Get(ctx, sql2.GetWarehouseRequest{Id: warehouseID})
	if err != nil {
		ui.PrintInfo(fmt.Sprintf("Warehouse ID: %s (Status: Unknown - %v)", warehouseID, err))
	} else {
		statusIcon := "❓"
		switch whInfo.State {
		case sql2.StateRunning:
			statusIcon = "✅"
		case sql2.StateStarting:
			statusIcon = "⏳"
		case sql2.StateStopped:
			statusIcon = "🛑"
		}
		ui.PrintInfo(fmt.Sprintf("Using Warehouse: %s %s (%s)", statusIcon, whInfo.Name, whInfo.State))
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s.%s LIMIT 5", c, s, t)
	ui.PrintInfo(fmt.Sprintf("Executing: %s", query))

	resp, err := w.StatementExecution.ExecuteAndWait(ctx, sql2.ExecuteStatementRequest{
		WarehouseId: warehouseID,
		Catalog:     c,
		Schema:      s,
		Statement:   query,
	})
	if err != nil {
		ui.PrintError(fmt.Sprintf("Query failed: %v", err))
		return
	}

	// Result handling
	if resp.Result == nil || resp.Result.DataArray == nil {
		ui.PrintInfo("No data found.")
		return
	}

	// Process columns
	var headers []string
	if resp.Manifest != nil && resp.Manifest.Schema != nil {
		for _, col := range resp.Manifest.Schema.Columns {
			headers = append(headers, col.Name)
		}
	} else {
		// Fallback or error?
		ui.PrintError("No schema manifest in response.")
		return
	}

	// Process rows
	// DataArray is [][]string
	ui.PrintTable(headers, resp.Result.DataArray)
}
