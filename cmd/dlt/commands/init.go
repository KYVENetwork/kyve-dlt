package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

func init() {
	initCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")
	rootCmd.AddCommand(initCmd)
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize dlt",
	Run: func(cmd *cobra.Command, args []string) {
		if err := utils.InitConfig(configPath); err != nil {
			logger.Error().Msg(err.Error())
			return
		}

		if !utils.PromptConfirm("\nDo you want to create a destination? [y/N]: ") {
			return
		}

		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		newDestination := utils.CreateDestinationEntry()
		utils.AddNodeToConfig(configNode, "destinations", &newDestination)
		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		sourceName := utils.SelectSource(configNode)
		if sourceName == "custom" {
			newSource := utils.CreateSourceEntry()
			utils.AddNodeToConfig(configNode, "sources", &newSource)
			if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
				logger.Error().Str("err", err.Error()).Msg("error saving config")
				return
			}
			sourceName = utils.GetNodeValue(newSource, "name")
		} else if sourceName == "" {
			logger.Error().Msg("no source selected")
			return
		}

		newConnection := utils.CreateConnectionEntry("connection_1", sourceName, utils.GetNodeValue(newDestination, "name"))
		utils.AddNodeToConfig(configNode, "connections", &newConnection)
		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		// Remove example destinations
		if err := utils.ClearConfig(configPath, "destinations", []string{"big_query_example", "postgres_example"}); err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to clear config template")
			return
		}

		// Remove example connections
		if err := utils.ClearConfig(configPath, "connections", []string{"connection_example"}); err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to clear config template")
			return
		}

		fmt.Println("\nSuccessfully initialized and created first connection \033[36m`connection_1`\033[0m!")

		fmt.Println("\nTo start a sync, run one of the following commands: \n" +
			"\033[32m" +
			"dlt sync --connection connection_1\n" +
			"dlt partial-sync --connection connection_1 --from-bundle-id 10 --to-bundle-id 20\n" +
			"dlt run --connection connection_1\n" +
			"\033[0m")

		fmt.Println("To manage your config, run one of the following commands: \n" +
			"\033[32m" +
			"dlt sources {add|remove|list}\n" +
			"dlt destinations {add|remove|list}\n" +
			"dlt connections {add|remove|list}" +
			"\033[0m")
	},
}
