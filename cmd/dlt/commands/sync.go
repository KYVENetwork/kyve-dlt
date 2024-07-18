package commands

import (
	"fmt"
	l "github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"math"
	"strings"
	"time"
)

func init() {
	syncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	syncCmd.Flags().StringVarP(&connection, "connections", "c", "", "name of the connections to sync (comma separated)")

	syncCmd.Flags().BoolVarP(&all, "all", "a", false, "sync all specified connections")

	syncCmd.Flags().Float64Var(&interval, "interval", 2, "interval of the sync process (in hours)")

	syncCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	syncCmd.Flags().BoolVarP(&force, "force", "f", false, "skips checks if data was already loaded in destination")

	rootCmd.AddCommand(syncCmd)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run a supervised incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Debug().Int64("from_bundle_id", fromBundleId).Float64("interval", interval).Msg("setting up supervised sync")
		if connection == "" && !all {
			logger.Error().Msg("either --connections or --all is required")
			return
		}

		config, err := utils.LoadConfig(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		var connections []string
		if all {
			c, err := utils.GetAllConnectionNames(config)
			if err != nil {
				logger.Error().Str("err", err.Error()).Msg("failed to get all connections")
				return
			}
			connections = *c
		} else {
			if connection == "" {
				logger.Error().Msg("either --connections or --all is required")
				return
			}
			connections = strings.Split(connection, ",")
		}

		sleepDuration := time.Duration(interval * float64(time.Hour))

		logger.Info().Int64("from_bundle_id", fromBundleId).Str("interval", fmt.Sprintf("%v hours", interval)).Msg("starting supervised incremental sync")

		for {
			for i := range connections {
				c := strings.TrimSpace(connections[i])

				loader, err := l.SetupLoader(configPath, c, false, fromBundleId, math.MaxInt64, force)
				if err != nil {
					logger.Error().Str("connection", c).Str("err", err.Error()).Msg("failed to set up loader")
					return
				}
				startTime := time.Now().Unix()

				logger.Info().Str("connection", c).Msg(fmt.Sprintf("Starting loading process"))

				loader.Start(true)

				logger.Info().Msg(fmt.Sprintf("Finished sync for %v! Took %d seconds", c, time.Now().Unix()-startTime))
			}

			logger.Info().Msg(fmt.Sprintf("Waiting %v hours before starting next sync", interval))
			time.Sleep(sleepDuration)
		}
	},
}
