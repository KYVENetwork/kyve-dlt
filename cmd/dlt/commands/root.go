package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

var (
	configPath string
	logger     = utils.DltLogger("cmd")
)

var rootCmd = &cobra.Command{
	Use:           "kyvedlt",
	Short:         "kyvedlt",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(fmt.Errorf("failed to execute root command: %w", err))
	}
}
