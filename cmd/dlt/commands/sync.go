package commands

import (
	"fmt"
	l "github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"math"
	"time"
)

func init() {
	syncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	syncCmd.Flags().StringVarP(&connection, "connection", "c", "", "name of the connection to sync")
	if err := syncCmd.MarkFlagRequired("connection"); err != nil {
		panic(fmt.Errorf("flag 'connection' should be required: %w", err))
	}

	syncCmd.Flags().Float64Var(&interval, "interval", 2, "interval of the sync process (in hours)")

	syncCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	syncCmd.Flags().BoolVarP(&force, "force", "f", false, "skips checks if data was already loaded in destination")

	rootCmd.AddCommand(syncCmd)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run a supervised incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("setting up supervised incremental sync")
		loader, err := l.SetupLoader(configPath, connection, false, fromBundleId, math.MaxInt64, force)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to set up loader")
			return
		}

		startTime := time.Now().Unix()

		sleepDuration := time.Duration(interval * float64(time.Hour))

		logger.Info().Int64("from_bundle_id", fromBundleId).Str("interval", fmt.Sprintf("%v hours", interval)).Msg("starting supervised incremental sync")

		for {
			loader.Start(true)
			logger.Info().Msg(fmt.Sprintf("Finished sync! Took %d seconds", time.Now().Unix()-startTime))

			logger.Info().Msg(fmt.Sprintf("Waiting %f hours before starting next sync", interval))
			time.Sleep(sleepDuration)
		}
	},
}
