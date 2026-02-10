package cmd

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dbx-explore",
	Short: "Databricks Unity Catalog Explorer CLI",
	Long:  `A CLI tool to explore Databricks Unity Catalog using SQL and REST API.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Load .env file if present
	_ = godotenv.Load()
}
