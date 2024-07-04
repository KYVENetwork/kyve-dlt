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

func ClearConfig(configPath string) error {
	configNode, err := LoadConfigWithComments(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Helper function to clear a node if it exists, otherwise create an empty sequence node
	clearOrInitializeNode := func(node *yaml.Node, key string) {
		for i, content := range node.Content[0].Content {
			if content.Value == key {
				// Clear the node's content
				node.Content[0].Content[i+1].Content = nil
				return
			}
		}
		// If the key doesn't exist, create it with an empty sequence node
		node.Content[0].Content = append(node.Content[0].Content, &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: key,
		}, &yaml.Node{
			Kind: yaml.SequenceNode,
		})
	}

	// Clear connections, sources, and destinations
	clearOrInitializeNode(configNode, "connections")
	clearOrInitializeNode(configNode, "sources")
	clearOrInitializeNode(configNode, "destinations")

	if err := SaveConfigWithComments(configPath, configNode); err != nil {
		return fmt.Errorf("error saving config: %w", err)
	}

	return nil
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
