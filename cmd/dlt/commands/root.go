package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

var (
	configPath   string
	fromBundleId int64
	logger       = utils.DltLogger("cmd")
	toBundleId   int64
)

var rootCmd = &cobra.Command{
	Use:           "dlt",
	Short:         "dlt",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(fmt.Errorf("failed to execute root command: %w", err))
	}
}
