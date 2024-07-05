package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"math"
	"time"

	_ "net/http/pprof"
)

func init() {
	syncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	syncCmd.Flags().StringVar(&connection, "connection", "", "name of the connection to sync")
	if err := syncCmd.MarkFlagRequired("connection"); err != nil {
		panic(fmt.Errorf("flag 'connection' should be required: %w", err))
	}

	syncCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	syncCmd.Flags().BoolVarP(&y, "yes", "y", false, "automatically answer yes for all questions")

	rootCmd.AddCommand(syncCmd)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Start the incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("setting up incremental sync")
		loader, err := setupLoader(configPath, false, fromBundleId, math.MaxInt64)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to set up loader")
			return
		}

		startTime := time.Now().Unix()

		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("starting incremental sync")

		loader.Start(y)

		logger.Info().Msg(fmt.Sprintf("Finished sync! Took %d seconds", time.Now().Unix()-startTime))
	},
}
