package utils

import (
	_ "embed"
	"fmt"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	logger = DltLogger("config")
)

//go:embed config_template.yml
var defaultConfig []byte

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
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		logger.Error().Str("err", err.Error()).Msg("failed to read config")
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		logger.Error().Str("err", err.Error()).Msg("failed to unmarshal config")
	}

	// Create default config if config doesn't exist
	if _, err := os.Stat(configPath); err != nil {
		logger.Info().Str("path", configPath).Msg("could not find config; creating with default values")

		dirPath := filepath.Dir(configPath)
		if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
			logger.Fatal().Str("directories", dirPath).Msg("failed to create directories")
			panic(err)
		}

		f, err := os.Create(configPath)
		if err != nil {
			logger.Fatal().Str("config-path", configPath).Msg("failed to create config file")
			panic(err)
		}

		_, err = f.Write(defaultConfig)
		if err != nil {
			logger.Fatal().Msg("failed to write default config")
		}

		logger.Info().Msg("created default config file, restart process")

		return nil, fmt.Errorf("could not find config file")
	}
	setLogLevel(config.LogLevel)

	return &config, nil
}

func LoadConfigWithComments(configPath string) (*yaml.Node, error) {
	data, err := ioutil.ReadFile(configPath)
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
