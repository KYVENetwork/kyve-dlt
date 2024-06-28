package utils

import (
	_ "embed"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

var (
	logger = DltLogger("config")
)

//go:embed config_template.yml
var defaultConfig []byte

func LoadConfig(configPath string) error {
	viper.SetConfigFile(configPath)

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

		return fmt.Errorf("could not find config file")
	}

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("failed to read config file: %w", err))
	}

	setLogLevel()
	return nil
}

func setLogLevel() {
	switch viper.GetString("log_level") {
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "none":
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}
}
