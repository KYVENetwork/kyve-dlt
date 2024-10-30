package commands

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	Version string
	Commit  string
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version and build information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("KYVE Data-Load-Tool (DLT)")
		fmt.Printf("Version: %s\n", Version)
		fmt.Printf("Commit: %s\n", Commit)
	},
}
