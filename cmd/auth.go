package cmd

import (
	"fmt"
	"os"

	"dbx-explore/pkg/auth"
	"dbx-explore/pkg/ui"

	"github.com/spf13/cobra"
)

// authCmd represents the auth command
var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Authentication and Setup",
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Interactive login setup",
	Run: func(cmd *cobra.Command, args []string) {
		if err := auth.RunInteractiveLogin(); err != nil {
			ui.PrintError(fmt.Sprintf("Login failed: %v", err))
			os.Exit(1)
		}
		ui.PrintInfo("You can now run 'dbx-explore catalog list-catalogs' etc.")
	},
}

func init() {
	rootCmd.AddCommand(authCmd)
	authCmd.AddCommand(loginCmd)
}
