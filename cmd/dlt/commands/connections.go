package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func init() {
	connectionsCmd.Flags().StringVar(&cfgPath, "config", "", "set custom config path")

	connectionsCmd.AddCommand(connectionsAddCmd)
	connectionsCmd.AddCommand(connectionsListCmd)
	connectionsCmd.AddCommand(connectionsRemoveCmd)

	rootCmd.AddCommand(connectionsCmd)
}

var connectionsCmd = &cobra.Command{
	Use:     "connections",
	Short:   "Add or remove a connection or list all",
	Aliases: []string{"c"},
}

var connectionsAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new connection",
	Run: func(cmd *cobra.Command, args []string) {
		configPath := utils.GetConfigPath(cfgPath)

		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		sourceName := utils.PromptInput("\033[36mEnter Source name: \033[0m")
		destName := utils.PromptInput("\033[36mEnter Destination name: \033[0m")

		if !valueExists(configNode, sourceName, "sources") {
			logger.Error().Str("source", sourceName).Msg("source does not exist")
			return
		}

		if !valueExists(configNode, destName, "destinations") {
			logger.Error().Str("destination", destName).Msg("destination does not exist")
			return
		}

		newConnection := yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Connection name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "source"},
				{Kind: yaml.ScalarNode, Value: sourceName},
				{Kind: yaml.ScalarNode, Value: "destination"},
				{Kind: yaml.ScalarNode, Value: destName},
				{Kind: yaml.ScalarNode, Value: "cron"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mSpecify cron schedule (e.g. '30 * * * *': \033[0m")},
			},
		}

		// Find the connections node
		var connectionsNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "connections" {
				connectionsNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Append the new connection
		if connectionsNode != nil {
			connectionsNode.Content = append(connectionsNode.Content, &newConnection)
		} else {
			// Handle case where connections node doesn't exist
			configNode.Content[0].Content = append(configNode.Content[0].Content, &yaml.Node{
				Kind:    yaml.ScalarNode,
				Value:   "connections",
				Tag:     "!!seq",
				Content: []*yaml.Node{&newConnection},
			})
		}

		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		logger.Info().Msg("Connection added successfully!")
	},
}

var connectionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all connections",
	Run: func(cmd *cobra.Command, args []string) {
		configPath := utils.GetConfigPath(cfgPath)

		config, err := utils.LoadConfig(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		if len(config.Connections) > 0 {
			columnOffset := 2
			maxNameLen, maxSourceLen, maxDestinationLen, maxCronLen := len("Name"), len("Source"), len("Destination"), len("Cron")
			for _, connection := range config.Connections {
				maxNameLen = max(maxNameLen, len(connection.Name)) + columnOffset
				maxSourceLen = max(maxSourceLen, len(fmt.Sprint(connection.Source))) + columnOffset
				maxDestinationLen = max(maxDestinationLen, len(fmt.Sprint(connection.Destination))) + columnOffset
				maxCronLen = max(maxCronLen, len(fmt.Sprint(connection.Cron))) + columnOffset
			}

			fmt.Printf("\033[36m%-*s %-*s %-*s %-*s\033[0m\n", maxNameLen, "Name", maxSourceLen, "Source", maxDestinationLen, "Destination", maxCronLen, "Cron")
			for _, connection := range config.Connections {
				fmt.Printf("%-*s %-*s %-*s %-*s\n", maxNameLen, connection.Name, maxSourceLen, connection.Source, maxDestinationLen, connection.Destination, maxCronLen, connection.Cron)
			}
		} else {
			fmt.Println("No connections defined.")
		}
	},
}

var connectionsRemoveCmd = &cobra.Command{
	Use:   "remove [connection name]",
	Short: "Remove a connection by name",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configPath := utils.GetConfigPath(cfgPath)

		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		connectionName := args[0]

		// Find the connections node
		var connectionsNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "connections" {
				connectionsNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Find and remove the connection by name
		if connectionsNode != nil {
			for i := 0; i < len(connectionsNode.Content); i++ {
				connection := connectionsNode.Content[i]
				var nameNode *yaml.Node
				for j := 0; j < len(connection.Content); j += 2 {
					if connection.Content[j].Value == "name" {
						nameNode = connection.Content[j+1]
						break
					}
				}
				if nameNode != nil && nameNode.Value == connectionName {
					connectionsNode.Content = append(connectionsNode.Content[:i], connectionsNode.Content[i+1:]...)
					if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
						logger.Error().Str("err", err.Error()).Msg("error saving config")
						return
					}
					logger.Info().Msg("Connection removed successfully!")
					return
				}
			}
			logger.Error().Msg("Connection not found.")
		} else {
			logger.Info().Msg("No connections defined.")
		}
	},
}

func valueExists(configNode *yaml.Node, sourceName, key string) bool {
	for i, node := range configNode.Content[0].Content {
		if node.Value == key {
			sourcesNode := configNode.Content[0].Content[i+1]
			for j := 0; j < len(sourcesNode.Content); j++ {
				source := sourcesNode.Content[j]
				for k := 0; k < len(source.Content); k += 2 {
					if source.Content[k].Value == "name" && source.Content[k+1].Value == sourceName {
						return true
					}
				}
			}
		}
	}
	return false
}
