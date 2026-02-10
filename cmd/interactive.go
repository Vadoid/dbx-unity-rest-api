package cmd

import (
	"context"
	"fmt"
	"os"

	"dbx-explore/pkg/auth"
	pkgcatalog "dbx-explore/pkg/catalog"
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

		menuItems := []string{"📂 Data Explorer"}
		if hasCreds {
			host := os.Getenv("DATABRICKS_HOST")
			ui.PrintInfo(fmt.Sprintf("Logged in as: %s", host))
			menuItems = append(menuItems, "🔌 Federation (Connections)")
			menuItems = append(menuItems, "🏗️ Infrastructure")
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

		if choice == "🔌 Federation (Connections)" {
			navigateFederation()
			continue
		}

		if choice == "🏗️ Infrastructure" {
			navigateInfrastructure()
			continue
		}

		// Start Exploration (Data Explorer)
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

		// Loop for Object Selection
		navigateObjects(ctx, w, catalogName, selectedSchema)
	}
}

func navigateObjects(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) {
	for {
		objectTypes := []string{
			"📋 Tables & Views",
			"📦 Volumes",
			"𝑓  Functions",
			"🤖 Models (Registered)",
			"⬅️  Back to Schemas",
		}

		_, choice, err := ui.SelectPrompt(fmt.Sprintf("Select Object Type (%s.%s)", catalogName, schemaName), objectTypes)
		if err != nil {
			return
		}

		switch choice {
		case "📋 Tables & Views":
			navigateTables(ctx, w, catalogName, schemaName)
		case "📦 Volumes":
			navigateVolumes(ctx, w, catalogName, schemaName)
		case "𝑓  Functions":
			navigateFunctions(ctx, w, catalogName, schemaName)
		case "🤖 Models (Registered)":
			navigateModels(ctx, w, catalogName, schemaName)
		case "⬅️  Back to Schemas":
			return
		}
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
			"🛡️ View Permissions",
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
		case "🛡️ View Permissions":
			showPermissions(ctx, w, "TABLE", fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName))
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

func navigateFederation() {
	ctx := context.Background()
	w := getWorkspaceClient()

	for {
		ui.PrintHeader("Federation (Connections)")
		conns, err := pkgcatalog.ListConnections(ctx, w)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Failed to list connections: %v", err))
			return
		}

		if len(conns) == 0 {
			ui.PrintInfo("No connections found.")
			return
		}

		items := make([]string, len(conns)+1)
		for i, c := range conns {
			items[i] = fmt.Sprintf("%s (%s)", c.Name, c.ConnectionType)
		}
		items[len(conns)] = "⬅️  Back to Main Menu"

		idx, choice, err := ui.SelectPrompt("Select Connection", items)
		if err != nil || choice == "⬅️  Back to Main Menu" {
			return
		}

		selected := conns[idx]
		ui.PrintKeyValue("Connection Details", map[string]string{
			"Name":      selected.Name,
			"Type":      string(selected.ConnectionType),
			"Owner":     selected.Owner,
			"Created":   fmt.Sprintf("%d", selected.CreatedAt),
			"Comment":   selected.Comment,
			"Url":       selected.Url,
			"Metastore": selected.MetastoreId,
		})
		fmt.Println("\nPress Enter to continue...")
		fmt.Scanln()
	}
}

func navigateInfrastructure() {
	ctx := context.Background()
	w := getWorkspaceClient()

	for {
		items := []string{
			"🏗️ Metastore Summary",
			"📦 External Locations",
			"🔑 Storage Credentials",
			"⬅️  Back to Main Menu",
		}

		_, choice, err := ui.SelectPrompt("Infrastructure", items)
		if err != nil || choice == "⬅️  Back to Main Menu" {
			return
		}

		switch choice {
		case "🏗️ Metastore Summary":
			showMetastoreSummary(ctx, w)
		case "📦 External Locations":
			listExternalLocations(ctx, w)
		case "🔑 Storage Credentials":
			listStorageCredentials(ctx, w)
		}
	}
}

func showMetastoreSummary(ctx context.Context, w *databricks.WorkspaceClient) {
	summary, err := pkgcatalog.GetMetastoreSummary(ctx, w)
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to get metastore summary: %v", err))
		return
	}

	ui.PrintHeader("Metastore Summary")
	ui.PrintKeyValue("Metastore Details", map[string]string{
		"Name":           summary.Name,
		"StorageRoot":    summary.StorageRoot,
		"Owner":          summary.Owner,
		"Region":         summary.Region,
		"Cloud":          summary.Cloud,
		"GlobalMetastoreId": summary.GlobalMetastoreId,
		"MetastoreId":    summary.MetastoreId,
	})
	fmt.Println("\nPress Enter to continue...")
	fmt.Scanln()
}

func listExternalLocations(ctx context.Context, w *databricks.WorkspaceClient) {
	locs, err := pkgcatalog.ListExternalLocations(ctx, w)
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to list external locations: %v", err))
		return
	}

	if len(locs) == 0 {
		ui.PrintInfo("No external locations found.")
		return
	}

	items := make([]string, len(locs)+1)
	for i, l := range locs {
		items[i] = l.Name
	}
	items[len(locs)] = "⬅️  Back"

	idx, choice, err := ui.SelectPrompt("Select External Location", items)
	if err != nil || choice == "⬅️  Back" {
		return
	}

	selected := locs[idx]
	ui.PrintKeyValue("External Location Details", map[string]string{
		"Name":           selected.Name,
		"Url":            selected.Url,
		"Owner":          selected.Owner,
		"Credential":     selected.CredentialName,
		"ReadOnly":       fmt.Sprintf("%v", selected.ReadOnly),
		"CreatedAt":      fmt.Sprintf("%d", selected.CreatedAt),
	})
	fmt.Println("\nPress Enter to continue...")
	fmt.Scanln()
}

func listStorageCredentials(ctx context.Context, w *databricks.WorkspaceClient) {
	creds, err := pkgcatalog.ListStorageCredentials(ctx, w)
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to list storage credentials: %v", err))
		return
	}

	if len(creds) == 0 {
		ui.PrintInfo("No storage credentials found.")
		return
	}

	items := make([]string, len(creds)+1)
	for i, c := range creds {
		items[i] = c.Name
	}
	items[len(creds)] = "⬅️  Back"

	idx, choice, err := ui.SelectPrompt("Select Credential", items)
	if err != nil || choice == "⬅️  Back" {
		return
	}

	selected := creds[idx]
	ui.PrintKeyValue("Storage Credential Details", map[string]string{
		"Name":      selected.Name,
		"Owner":     selected.Owner,
		"CreatedAt": fmt.Sprintf("%d", selected.CreatedAt),
	})
	fmt.Println("\nPress Enter to continue...")
	fmt.Scanln()
}

func showPermissions(ctx context.Context, w *databricks.WorkspaceClient, securableType, fullName string) {
	ui.PrintInfo(fmt.Sprintf("Fetching permissions for %s (%s)...", fullName, securableType))
	perms, err := pkgcatalog.GetEffectivePermissions(ctx, w, securableType, fullName)
	if err != nil {
		ui.PrintError(fmt.Sprintf("Failed to get permissions: %v", err))
		return
	}

	if len(perms.PrivilegeAssignments) == 0 {
		ui.PrintInfo("No permissions found.")
		fmt.Println("\nPress Enter to continue...")
		fmt.Scanln()
		return
	}

	var rows [][]string
	for _, p := range perms.PrivilegeAssignments {
		for _, priv := range p.Privileges {
			inheritedFrom := "Direct"
			if priv.InheritedFromType != "" {
				inheritedFrom = fmt.Sprintf("%s (%s)", priv.InheritedFromType, priv.InheritedFromName)
			}
			rows = append(rows, []string{p.Principal, string(priv.Privilege), inheritedFrom})
		}
	}

	ui.PrintTable([]string{"Principal", "Privilege", "Inherited From"}, rows)
	fmt.Println("\nPress Enter to continue...")
	fmt.Scanln()
}

func navigateVolumes(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) {
	for {
		vols, err := pkgcatalog.ListVolumes(ctx, w, catalogName, schemaName)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Failed to list volumes: %v", err))
			return
		}

		if len(vols) == 0 {
			ui.PrintInfo("No volumes found.")
			return
		}

		items := make([]string, len(vols)+1)
		for i, v := range vols {
			items[i] = fmt.Sprintf("%s (%s)", v.Name, v.VolumeType)
		}
		items[len(vols)] = "⬅️  Back"

		idx, choice, err := ui.SelectPrompt("Select Volume", items)
		if err != nil || choice == "⬅️  Back" {
			return
		}

		selected := vols[idx]
		navigateVolumeActions(ctx, w, selected)
	}
}

func navigateVolumeActions(ctx context.Context, w *databricks.WorkspaceClient, vol catalog.VolumeInfo) {
	for {
		ui.PrintHeader(fmt.Sprintf("Volume: %s", vol.Name))
		actions := []string{
			"📄 View Details",
			"🛡️ View Permissions",
			"⬅️  Back to Volumes",
		}

		_, choice, err := ui.SelectPrompt("Choose Action", actions)
		if err != nil {
			return
		}

		switch choice {
		case "📄 View Details":
			ui.PrintKeyValue("Volume Details", map[string]string{
				"Name":            vol.Name,
				"Type":            string(vol.VolumeType),
				"Owner":           vol.Owner,
				"StorageLocation": vol.StorageLocation,
				"Comment":         vol.Comment,
			})
			fmt.Println("\nPress Enter to continue...")
			fmt.Scanln()
		case "🛡️ View Permissions":
			showPermissions(ctx, w, "VOLUME", vol.FullName)
		case "⬅️  Back to Volumes":
			return
		}
	}
}

func navigateFunctions(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) {
	for {
		funcs, err := pkgcatalog.ListFunctions(ctx, w, catalogName, schemaName)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Failed to list functions: %v", err))
			return
		}

		if len(funcs) == 0 {
			ui.PrintInfo("No functions found.")
			return
		}

		items := make([]string, len(funcs)+1)
		for i, f := range funcs {
			items[i] = fmt.Sprintf("%s", f.Name)
		}
		items[len(funcs)] = "⬅️  Back"

		idx, choice, err := ui.SelectPrompt("Select Function", items)
		if err != nil || choice == "⬅️  Back" {
			return
		}

		selected := funcs[idx]
		// Fetch full details including routine definition
		fullFunc, err := pkgcatalog.GetFunction(ctx, w, selected.FullName)
		if err == nil {
			selected = *fullFunc
		}

		navigateFunctionActions(ctx, w, selected)
	}
}

func navigateFunctionActions(ctx context.Context, w *databricks.WorkspaceClient, fn catalog.FunctionInfo) {
	for {
		ui.PrintHeader(fmt.Sprintf("Function: %s", fn.Name))
		actions := []string{
			"📄 View Details",
			"🛡️ View Permissions",
			"⬅️  Back to Functions",
		}

		_, choice, err := ui.SelectPrompt("Choose Action", actions)
		if err != nil {
			return
		}

		switch choice {
		case "📄 View Details":
			ui.PrintKeyValue("Function Details", map[string]string{
				"Name":            fn.Name,
				"DataType":        string(fn.DataType),
				"Owner":           fn.Owner,
				"RoutineBody":     string(fn.RoutineBody),
				"IsDeterministic": fmt.Sprintf("%v", fn.IsDeterministic),
				"Comment":         fn.Comment,
			})

			if fn.RoutineDefinition != "" {
				fmt.Println("\n📜 Routine Definition:")
				fmt.Println("--------------------------------------------------")
				fmt.Println(fn.RoutineDefinition)
				fmt.Println("--------------------------------------------------")
			}
			fmt.Println("\nPress Enter to continue...")
			fmt.Scanln()
		case "🛡️ View Permissions":
			showPermissions(ctx, w, "FUNCTION", fn.FullName)
		case "⬅️  Back to Functions":
			return
		}
	}
}

func navigateModels(ctx context.Context, w *databricks.WorkspaceClient, catalogName, schemaName string) {
	for {
		models, err := pkgcatalog.ListModels(ctx, w, catalogName, schemaName)
		if err != nil {
			ui.PrintError(fmt.Sprintf("Failed to list models: %v", err))
			return
		}

		if len(models) == 0 {
			ui.PrintInfo("No models found.")
			return
		}

		items := make([]string, len(models)+1)
		for i, m := range models {
			items[i] = m.Name
		}
		items[len(models)] = "⬅️  Back"

		idx, choice, err := ui.SelectPrompt("Select Model", items)
		if err != nil || choice == "⬅️  Back" {
			return
		}

		selected := models[idx]
		navigateModelActions(ctx, w, selected)
	}
}

func navigateModelActions(ctx context.Context, w *databricks.WorkspaceClient, model catalog.RegisteredModelInfo) {
	for {
		ui.PrintHeader(fmt.Sprintf("Model: %s", model.Name))
		actions := []string{
			"📄 View Details",
			"🛡️ View Permissions",
			"⬅️  Back to Models",
		}

		_, choice, err := ui.SelectPrompt("Choose Action", actions)
		if err != nil {
			return
		}

		switch choice {
		case "📄 View Details":
			ui.PrintKeyValue("Model Details", map[string]string{
				"Name":      model.Name,
				"Owner":     model.Owner,
				"Comment":   model.Comment,
				"CreatedAt": fmt.Sprintf("%d", model.CreatedAt),
				"UpdatedAt": fmt.Sprintf("%d", model.UpdatedAt),
			})
			fmt.Println("\nPress Enter to continue...")
			fmt.Scanln()
		case "🛡️ View Permissions":
			showPermissions(ctx, w, "FUNCTION", model.FullName) // Models are securable type FUNCTION in SDK? Need to check.
		case "⬅️  Back to Models":
			return
		}
	}
}
