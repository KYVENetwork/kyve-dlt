package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

func init() {
	sourcesCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	sourcesCmd.AddCommand(sourcesListCmd)

	rootCmd.AddCommand(sourcesCmd)
}

var sourcesCmd = &cobra.Command{
	Use:     "sources",
	Short:   "Add or remove a source or list all",
	Aliases: []string{"s"},
}

var sourcesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all specified sources",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			return
		}

		if len(config.Sources) > 0 {
			columnOffset := 2
			maxNameLen, maxPoolIDLen, maxStepSizeLen, maxEndpointLen, maxSchemaLen := len("Name"), len("PoolID"), len("StepSize"), len("Endpoint"), len("Schema")
			for _, source := range config.Sources {
				maxNameLen = max(maxNameLen, len(source.Name)) + columnOffset
				maxPoolIDLen = max(maxPoolIDLen, len(fmt.Sprint(source.PoolID))) + columnOffset
				maxStepSizeLen = max(maxStepSizeLen, len(fmt.Sprint(source.StepSize))) + columnOffset
				maxEndpointLen = max(maxEndpointLen, len(source.Endpoint)) + columnOffset
				maxSchemaLen = max(maxSchemaLen, len(source.Schema))
			}

			fmt.Printf("\033[36m%-*s %-*s %-*s %-*s %-*s\033[0m\n", maxNameLen, "Name", maxPoolIDLen, "PoolID", maxStepSizeLen, "StepSize", maxEndpointLen, "Endpoint", maxSchemaLen, "Schema")
			for _, source := range config.Sources {
				fmt.Printf("%-*s %-*d %-*d %-*s %-*s\n", maxNameLen, source.Name, maxPoolIDLen, source.PoolID, maxStepSizeLen, source.StepSize, maxEndpointLen, source.Endpoint, maxSchemaLen, source.Schema)
			}
		} else {
			fmt.Println("No sources defined.")
		}
	},
}
