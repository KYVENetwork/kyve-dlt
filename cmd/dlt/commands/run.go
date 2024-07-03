package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"math"
	"time"
)

func init() {
	runCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	runCmd.Flags().StringVar(&connection, "connection", "", "name of the connection to sync")
	if err := runCmd.MarkFlagRequired("connection"); err != nil {
		panic(fmt.Errorf("flag 'connection' should be required: %w", err))
	}

	runCmd.Flags().Int64Var(&interval, "interval", 2, "interval of the sync process (in hours)")

	runCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	runCmd.Flags().BoolVarP(&y, "yes", "y", false, "automatically answer yes for all questions")

	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a supervised incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("setting up supervised incremental sync")
		loader, err := setupLoader(configPath, false, fromBundleId, math.MaxInt64)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to set up loader")
			return
		}

		startTime := time.Now().Unix()

		logger.Info().Int64("from_bundle_id", fromBundleId).Str("interval", fmt.Sprintf("%v hours", interval)).Msg("starting supervised incremental sync")

		for {
			loader.Start(y)
			logger.Info().Msg(fmt.Sprintf("Finished sync! Took %d seconds", time.Now().Unix()-startTime))

			logger.Info().Msg(fmt.Sprintf("Waiting %d hours before starting next sync", interval))
			time.Sleep(time.Duration(interval) * time.Hour)
		}
	},
}
