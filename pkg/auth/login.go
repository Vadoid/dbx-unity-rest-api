package auth

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"dbx-explore/pkg/ui"
)

// RunInteractiveLogin performs the full interactive login flow.
func RunInteractiveLogin() error {
	ui.PrintInfo("Starting Databricks Setup...")
	reader := bufio.NewReader(os.Stdin)

	// 1. Get Host
	fmt.Print("Enter your Databricks Host (e.g., https://<id>.cloud.databricks.com): ")
	host, _ := reader.ReadString('\n')
	host = strings.TrimSpace(host)
	host = strings.TrimSuffix(host, "/")

	if host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	// 2. Open Browser
	// The path /#setting/account usually lands on User Settings.
	// Deep linking to tokens is flaky.
	tokenURL := fmt.Sprintf("%s/#setting/account", host)
	ui.PrintInfo("Opening browser to 'User Settings'...")
	ui.PrintInfo("👉 Action Required: Click on the 'Developer' tab (or 'Access tokens') to generate a new token.")
	ui.PrintInfo(fmt.Sprintf("Link: %s", tokenURL))
	openBrowser(tokenURL)

	// 3. Get Token
	fmt.Print("Enter your Personal Access Token: ")
	token, _ := reader.ReadString('\n')
	token = strings.TrimSpace(token)

	if token == "" {
		return fmt.Errorf("token cannot be empty")
	}

	// 4. Auto-Discovery
	ui.PrintInfo("Auto-discovering SQL Warehouses...")
	warehouse, err := DiscoverBestWarehouse(context.Background(), host, token)
	httpPath := ""
	if err != nil {
		ui.PrintError(fmt.Sprintf("Auto-discovery failed: %v", err))
		fmt.Print("Enter SQL HTTP Path manually: ")
		httpPath, _ = reader.ReadString('\n')
		httpPath = strings.TrimSpace(httpPath)
	} else {
		ui.PrintSuccess(fmt.Sprintf("Selected SQL Warehouse: %s", warehouse.Name))
		if warehouse.OdbcParams != nil {
			httpPath = warehouse.OdbcParams.Path
		}
		if httpPath == "" {
			ui.PrintError("Could not determine HTTP Path from warehouse metadata.")
		}
	}

	// 5. Save to .env
	f, err := os.Create(".env")
	if err != nil {
		return fmt.Errorf("failed to create .env file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("DATABRICKS_HOST=%s\n", host)); err != nil {
		return err
	}
	if _, err := f.WriteString(fmt.Sprintf("DATABRICKS_TOKEN=%s\n", token)); err != nil {
		return err
	}
	if httpPath != "" {
		if _, err := f.WriteString(fmt.Sprintf("DATABRICKS_HTTP_PATH=%s\n", httpPath)); err != nil {
			return err
		}
	}
	if warehouse.Id != "" {
		if _, err := f.WriteString(fmt.Sprintf("DATABRICKS_WAREHOUSE_ID=%s\n", warehouse.Id)); err != nil {
			return err
		}
	}

	ui.PrintSuccess("Credentials saved to .env")
	// Re-load env vars for current process
	os.Setenv("DATABRICKS_HOST", host)
	os.Setenv("DATABRICKS_TOKEN", token)
	if httpPath != "" {
		os.Setenv("DATABRICKS_HTTP_PATH", httpPath)
	}
	if warehouse.Id != "" {
		os.Setenv("DATABRICKS_WAREHOUSE_ID", warehouse.Id)
	}

	return nil
}

func openBrowser(url string) {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		ui.PrintError(fmt.Sprintf("Could not open browser: %v", err))
	}
}

// ClearCredentials removes the .env file and unsets environment variables.
func ClearCredentials() error {
	// Unset env vars
	os.Unsetenv("DATABRICKS_HOST")
	os.Unsetenv("DATABRICKS_TOKEN")
	os.Unsetenv("DATABRICKS_HTTP_PATH")
	os.Unsetenv("DATABRICKS_WAREHOUSE_ID")

	// Remove .env file
	if err := os.Remove(".env"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove .env file: %w", err)
	}

	ui.PrintSuccess("Credentials cleared.")
	return nil
}

// UpdateEnvWarehouse updates the DATABRICKS_WAREHOUSE_ID in .env and os environment.
func UpdateEnvWarehouse(warehouseID string) error {
	// 1. Read existing .env
	content, err := os.ReadFile(".env")
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read .env: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	newLines := []string{}
	found := false

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "DATABRICKS_WAREHOUSE_ID=") {
			newLines = append(newLines, fmt.Sprintf("DATABRICKS_WAREHOUSE_ID=%s", warehouseID))
			found = true
		} else {
			newLines = append(newLines, line)
		}
	}

	if !found {
		newLines = append(newLines, fmt.Sprintf("DATABRICKS_WAREHOUSE_ID=%s", warehouseID))
	}

	// 2. Write back to .env
	if err := os.WriteFile(".env", []byte(strings.Join(newLines, "\n")+"\n"), 0644); err != nil {
		return fmt.Errorf("failed to update .env: %w", err)
	}

	// 3. Update current process env
	os.Setenv("DATABRICKS_WAREHOUSE_ID", warehouseID)
	return nil
}
