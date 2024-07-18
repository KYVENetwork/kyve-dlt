package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func init() {
	sourcesCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	sourcesCmd.AddCommand(sourcesAddCmd)
	sourcesCmd.AddCommand(sourcesListCmd)
	sourcesCmd.AddCommand(sourcesRemoveCmd)

	rootCmd.AddCommand(sourcesCmd)
}

var sourcesCmd = &cobra.Command{
	Use:     "sources",
	Short:   "Add or remove a source or list all",
	Aliases: []string{"s"},
}

var sourcesAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new source",
	Run: func(cmd *cobra.Command, args []string) {
		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		newSource := yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Source name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "pool_id"},
				{Kind: yaml.ScalarNode, Value: utils.PromptPoolId("\033[36mEnter KYVE Pool ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "batch_size"},
				{Kind: yaml.ScalarNode, Value: utils.PromptBatchSize("\033[36mEnter batch size [default 20]: \033[0m", "20")},
				{Kind: yaml.ScalarNode, Value: "endpoint"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter endpoint [default https://api.kyve.network]: \033[0m", "https://api.kyve.network")},
				{Kind: yaml.ScalarNode, Value: "schema"},
				{Kind: yaml.ScalarNode, Value: utils.PromptSchemaDropdown("\033[36mSelect schema: \033[0m", []string{"base", "tendermint", "tendermint_preprocessed"})},
			},
		}

		// Find the sources node
		var sourcesNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "sources" {
				sourcesNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Append the new source
		if sourcesNode != nil {
			sourcesNode.Content = append(sourcesNode.Content, &newSource)
		} else {
			// Handle case where sources node doesn't exist
			configNode.Content[0].Content = append(configNode.Content[0].Content, &yaml.Node{
				Kind:    yaml.ScalarNode,
				Value:   "sources",
				Tag:     "!!seq",
				Content: []*yaml.Node{&newSource},
			})
		}

		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		logger.Info().Msg("Source added successfully!")
	},
}

var sourcesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all specified sources",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		if len(config.Sources) > 0 {
			columnOffset := 2
			maxNameLen, maxPoolIDLen, maxBatchSize, maxEndpointLen, maxSchemaLen := len("Name"), len("PoolID"), len("BatchSize"), len("Endpoint"), len("Schema")
			for _, source := range config.Sources {
				maxNameLen = max(maxNameLen, len(source.Name)) + columnOffset
				maxPoolIDLen = max(maxPoolIDLen, len(fmt.Sprint(source.PoolID))) + columnOffset
				maxBatchSize = max(maxBatchSize, len(fmt.Sprint(source.BatchSize))) + columnOffset
				maxEndpointLen = max(maxEndpointLen, len(source.Endpoint)) + columnOffset
				maxSchemaLen = max(maxSchemaLen, len(source.Schema))
			}

			fmt.Printf("\033[36m%-*s %-*s %-*s %-*s %-*s\033[0m\n", maxNameLen, "Name", maxPoolIDLen, "PoolID", maxBatchSize, "BatchSize", maxEndpointLen, "Endpoint", maxSchemaLen, "Schema")
			for _, source := range config.Sources {
				fmt.Printf("%-*s %-*d %-*d %-*s %-*s\n", maxNameLen, source.Name, maxPoolIDLen, source.PoolID, maxBatchSize, source.BatchSize, maxEndpointLen, source.Endpoint, maxSchemaLen, source.Schema)
			}
		} else {
			fmt.Println("No sources defined.")
		}
	},
}

var sourcesRemoveCmd = &cobra.Command{
	Use:   "remove [source name]",
	Short: "Remove a source by name",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		sourceName := args[0]

		// Find the sources node
		var sourcesNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "sources" {
				sourcesNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Find and remove the source by name
		if sourcesNode != nil {
			for i := 0; i < len(sourcesNode.Content); i++ {
				source := sourcesNode.Content[i]
				for j := 0; j < len(source.Content); j += 2 {
					if source.Content[j].Value == "name" && source.Content[j+1].Value == sourceName {
						sourcesNode.Content = append(sourcesNode.Content[:i], sourcesNode.Content[i+1:]...)
						if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
							logger.Error().Str("err", err.Error()).Msg("error saving config")
							return
						}
						logger.Info().Msg("Source removed successfully!")
						return
					}
				}
			}
			logger.Error().Msg("Source not found.")
		} else {
			logger.Info().Msg("No sources defined.")
		}
	},
}
