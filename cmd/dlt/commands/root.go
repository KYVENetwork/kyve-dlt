package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

var (
	all          bool
	configPath   string
	connection   string
	force        bool
	fromBundleId int64
	interval     float64
	logger       = utils.DltLogger("cmd")
	toBundleId   int64
	y            bool
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
