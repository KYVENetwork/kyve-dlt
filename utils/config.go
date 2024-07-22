package utils

import (
	_ "embed"
	"fmt"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

var (
	logger = DltLogger("config")
)

//go:embed config_template.yml
var defaultConfig []byte

func AddNodeToConfig(configNode *yaml.Node, key string, newNode *yaml.Node) {
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

func ClearConfig(configPath, section string, namesToRemove []string) error {
	configNode, err := LoadConfigWithComments(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Find the target section in the config
	var targetNode *yaml.Node
	for i, node := range configNode.Content[0].Content {
		if node.Value == section {
			targetNode = configNode.Content[0].Content[i+1]
			break
		}
	}

	if targetNode == nil {
		return fmt.Errorf("section %s not found in the config", section)
	}

	// Filter out entries that match names in namesToRemove
	var filteredContent []*yaml.Node
	for _, entryNode := range targetNode.Content {
		name := GetNodeValue(*entryNode, "name")
		if !Contains(namesToRemove, name) {
			filteredContent = append(filteredContent, entryNode)
		}
	}

	// Update the target section with filtered content
	targetNode.Content = filteredContent

	// Save the modified config back to the file
	if err := SaveConfigWithComments(configPath, configNode); err != nil {
		return fmt.Errorf("error saving config: %w", err)
	}

	return nil
}

func Contains(slice []string, item string) bool {
	for _, elem := range slice {
		if elem == item {
			return true
		}
	}
	return false
}

func CreateConnectionEntry(connectionName, sourceName, destName string) yaml.Node {
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

func CreateDestinationEntry() yaml.Node {
	destinationType := PromptDestinationDropdown("\033[36mAvailable options: \033[0m", []string{"big_query", "postgres"})

	switch destinationType {
	case "big_query":
		return yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Destination name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "type"},
				{Kind: yaml.ScalarNode, Value: "big_query"},
				{Kind: yaml.ScalarNode, Value: "project_id"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Project ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "dataset_id"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Dataset ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "table_id"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Table ID: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "bucket_name"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Bucket Name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "worker_count"},
				{Kind: yaml.ScalarNode, Value: PromptInputWithDefault("\033[36mEnter Worker count (default 2): \033[0m", "2")},
				{Kind: yaml.ScalarNode, Value: "bucket_worker_count"},
				{Kind: yaml.ScalarNode, Value: PromptInputWithDefault("\033[36mEnter Bucket Worker count (default 2): \033[0m", "2")},
			},
		}
	case "postgres":
		return yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "name"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Destination name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "type"},
				{Kind: yaml.ScalarNode, Value: "postgres"},
				{Kind: yaml.ScalarNode, Value: "connection_url"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Connection URL: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "table_name"},
				{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Table name: \033[0m")},
				{Kind: yaml.ScalarNode, Value: "worker_count"},
				{Kind: yaml.ScalarNode, Value: PromptInputWithDefault("\033[36mEnter Worker count (default 4): \033[0m", "4")},
			},
		}
	default:
		return yaml.Node{}
	}
}

func CreateSourceEntry() yaml.Node {
	return yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: PromptInput("\033[36mEnter Source name: \033[0m")},
			{Kind: yaml.ScalarNode, Value: "pool_id"},
			{Kind: yaml.ScalarNode, Value: PromptPoolId("\033[36mEnter KYVE Pool ID: \033[0m")},
			{Kind: yaml.ScalarNode, Value: "batch_size"},
			{Kind: yaml.ScalarNode, Value: PromptBatchSize("\033[36mEnter batch size [default 20]: \033[0m", "20")},
			{Kind: yaml.ScalarNode, Value: "endpoint"},
			{Kind: yaml.ScalarNode, Value: PromptInputWithDefault("\033[36mEnter endpoint [default https://api.kyve.network]: \033[0m", "https://api.kyve.network")},
			{Kind: yaml.ScalarNode, Value: "schema"},
			{Kind: yaml.ScalarNode, Value: PromptSchemaDropdown("\033[36mSelect schema: \033[0m", []string{"base", "tendermint", "tendermint_preprocessed"})},
		},
	}
}

func GetAllConnectionNames(config *Config) (*[]Connection, error) {
	var connections []Connection
	for _, connection := range config.Connections {
		connections = append(connections, connection)
	}
	if len(connections) == 0 {
		return nil, fmt.Errorf("no connections defined")
	}
	return &connections, nil
}

func GetConnectionDetails(config *Config, connectionName string) (Source, Destination, error) {
	var source Source
	var destination Destination
	var connectionFound, sourceFound, destinationFound bool
	var sourceName, destinationName string

	for _, connection := range config.Connections {
		if connection.Name == connectionName {
			connectionFound = true
			sourceName = connection.Source
			destinationName = connection.Destination
			for _, src := range config.Sources {
				if src.Name == sourceName {
					source = src
					sourceFound = true
					break
				}
			}
			for _, dst := range config.Destinations {
				if dst.Name == destinationName {
					destination = dst
					destinationFound = true
					break
				}
			}
		}
	}

	if !connectionFound {
		return Source{}, Destination{}, fmt.Errorf("connection %s not found", connectionName)
	}

	if !sourceFound {
		return Source{}, Destination{}, fmt.Errorf("source %s not found for connection %s", sourceName, connectionName)
	}

	if !destinationFound {
		return Source{}, Destination{}, fmt.Errorf("destination %s not found for connection %s", destinationName, connectionName)
	}

	return source, destination, nil
}

func GetNodeValue(node yaml.Node, key string) string {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1].Value
		}
	}
	return ""
}

func InitConfig(configPath string) error {
	// Create default config if config doesn't exist
	if _, err := os.Stat(configPath); err != nil {
		logger.Info().Str("path", configPath).Msg("creating default config")

		dirPath := filepath.Dir(configPath)
		if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
			logger.Error().Str("directories", dirPath).Msg("failed to create directories")
			panic(err)
		}

		f, err := os.Create(configPath)
		if err != nil {
			logger.Error().Str("config-path", configPath).Msg("failed to create config file")
			panic(err)
		}

		_, err = f.Write(defaultConfig)
		if err != nil {
			logger.Error().Msg("failed to write default config")
		}
		return nil
	}
	return fmt.Errorf("already initialized")
}

func LoadConfig(configPath string) (*Config, error) {
	// Create default config if config doesn't exist
	if _, err := os.Stat(configPath); err != nil {
		logger.Info().Str("path", configPath).Msg("could not find config; creating with default values")

		dirPath := filepath.Dir(configPath)
		if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
			logger.Error().Str("directories", dirPath).Msg("failed to create directories")
			panic(err)
		}

		f, err := os.Create(configPath)
		if err != nil {
			logger.Error().Str("config-path", configPath).Msg("failed to create config file")
			panic(err)
		}

		_, err = f.Write(defaultConfig)
		if err != nil {
			logger.Error().Msg("failed to write default config")
		}
		return nil, fmt.Errorf("created default config, restart process")
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Error().Str("err", err.Error()).Msg("failed to read config")
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		logger.Error().Str("err", err.Error()).Msg("failed to unmarshal config")
	}
	setLogLevel(config.LogLevel)

	return &config, nil
}

func LoadConfigWithComments(configPath string) (*yaml.Node, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}

	return &node, nil
}

func SaveConfigWithComments(path string, node *yaml.Node) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	if err := encoder.Encode(node); err != nil {
		return err
	}

	return nil
}

func setLogLevel(logLevel string) {
	switch logLevel {
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "none":
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}
}
