package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
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

		if !utils.PromptConfirm("\nDo you want to proceed with the initialization? [y/N]: ") {
			return
		}

		if err := utils.ClearConfig(configPath); err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to clear config template")
			return
		}

		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		fmt.Println("\033[36m1) Create a Source\033[0m")
		newSource := createSourceEntry()
		addNodeToConfig(configNode, "sources", &newSource)
		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		fmt.Println("\n\033[36m2) Create a Destination\033[0m")
		configNode, err = utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		newDestination := createDestinationEntry()
		addNodeToConfig(configNode, "destinations", &newDestination)
		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		newConnection := createConnectionEntry("connection_1", getNodeValue(newSource, "name"), getNodeValue(newDestination, "name"))
		addNodeToConfig(configNode, "connections", &newConnection)
		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}
		fmt.Println("\n\033[36mSuccessfully initialized and created first connection `connection_1`!\033[0m")
	},
}

func createConnectionEntry(connectionName, sourceName, destName string) yaml.Node {
	return yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: connectionName},
			{Kind: yaml.ScalarNode, Value: "source"},
			{Kind: yaml.ScalarNode, Value: sourceName},
			{Kind: yaml.ScalarNode, Value: "destination"},
			{Kind: yaml.ScalarNode, Value: destName},
		},
	}
}

func createDestinationEntry() yaml.Node {
	destinationType := utils.PromptDestinationDropdown("\033[36mSelect destination: \033[0m", []string{"big_query", "postgres"})

	switch destinationType {
	case "big_query":
		return yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Destination name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "type"},
				{Kind: yaml.ScalarNode, Value: "big_query"},
				{Kind: yaml.ScalarNode, Value: "project_id"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Project ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "dataset_id"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Dataset ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "table_id"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Table ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "worker_count"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Worker count (default 2): \033[0m", "2")},
				{Kind: yaml.ScalarNode, Value: "bucket_worker_count"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Bucket Worker count (default 2): \033[0m", "2")},
			},
		}
	case "postgres":
		return yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Destination name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "type"},
				{Kind: yaml.ScalarNode, Value: "postgres"},
				{Kind: yaml.ScalarNode, Value: "connection_url"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Connection URL: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "table_name"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Table name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "worker_count"},
				{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Worker count (default 4): \033[0m", "4")},
			},
		}
	default:
		return yaml.Node{}
	}
}

func createSourceEntry() yaml.Node {
	return yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Source name: \033[0m")},
			{Kind: yaml.ScalarNode, Value: "pool_id"},
			{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter KYVE Pool ID: \033[0m")},
			{Kind: yaml.ScalarNode, Value: "step_size"},
			{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter step size [default 20]: \033[0m", "20")},
			{Kind: yaml.ScalarNode, Value: "endpoint"},
			{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter endpoint [default https://api.kyve.network]: \033[0m", "https://api.kyve.network")},
			{Kind: yaml.ScalarNode, Value: "schema"},
			{Kind: yaml.ScalarNode, Value: utils.PromptSchemaDropdown("\033[36mSelect schema: \033[0m", []string{"base", "tendermint", "tendermint_preprocessed"})},
		},
	}
}

func addNodeToConfig(configNode *yaml.Node, key string, newNode *yaml.Node) {
	var targetNode *yaml.Node
	for i, node := range configNode.Content[0].Content {
		if node.Value == key {
			targetNode = configNode.Content[0].Content[i+1]
			break
		}
	}

	if targetNode != nil {
		targetNode.Content = append(targetNode.Content, newNode)
	} else {
		configNode.Content[0].Content = append(configNode.Content[0].Content, &yaml.Node{
			Kind:    yaml.ScalarNode,
			Value:   key,
			Tag:     "!!seq",
			Content: []*yaml.Node{newNode},
		})
	}
}

func getNodeValue(node yaml.Node, key string) string {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1].Value
		}
	}
	return ""
}
