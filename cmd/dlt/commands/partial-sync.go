package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"time"

	_ "net/http/pprof"
)

func init() {
	partialSyncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	partialSyncCmd.Flags().StringVar(&connection, "connection", "", "name of the connection to sync")
	if err := partialSyncCmd.MarkFlagRequired("connection"); err != nil {
		panic(fmt.Errorf("flag 'connection' should be required: %w", err))
	}

	partialSyncCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "ID of first bundle to load (inclusive)")
	if err := partialSyncCmd.MarkFlagRequired("from-bundle-id"); err != nil {
		panic(fmt.Errorf("flag 'from-bundle-id' should be required: %w", err))
	}

	partialSyncCmd.Flags().Int64Var(&toBundleId, "to-bundle-id", 0, "ID of last bundle to load (inclusive)")
	if err := partialSyncCmd.MarkFlagRequired("to-bundle-id"); err != nil {
		panic(fmt.Errorf("flag 'to-bundle-id' should be required: %w", err))
	}

	partialSyncCmd.Flags().BoolVarP(&y, "yes", "y", false, "automatically answer yes for all questions")

	rootCmd.AddCommand(partialSyncCmd)
}

var partialSyncCmd = &cobra.Command{
	Use:   "partial-sync",
	Short: "Load a specific range of bundles",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info().Int64("from_bundle_id", fromBundleId).Int64("to_bundle_id", toBundleId).Msg("setting up partial sync")
		loader, err := setupLoader(configPath, true, fromBundleId, toBundleId)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to set up loader")
			return
		}

		startTime := time.Now().Unix()

		logger.Info().Int64("from_bundle_id", fromBundleId).Int64("to_bundle_id", toBundleId).Msg("starting partial sync")

		loader.Start(y)

		logger.Info().Msg(fmt.Sprintf("Finished sync! Took %d seconds", time.Now().Unix()-startTime))
	},
}
