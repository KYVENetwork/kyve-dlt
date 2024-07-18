package commands

import (
	"fmt"
	l "github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"time"

	_ "net/http/pprof"
)

var (
	setTo = false
)

func init() {
	loadCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	loadCmd.Flags().StringVarP(&connection, "connection", "c", "", "name of the connection to sync")
	if err := loadCmd.MarkFlagRequired("connection"); err != nil {
		panic(fmt.Errorf("flag 'connection' should be required: %w", err))
	}

	loadCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	loadCmd.Flags().Int64Var(&toBundleId, "to-bundle-id", 0, "ID of last bundle to load (inclusive)")

	loadCmd.Flags().BoolVarP(&force, "force", "f", false, "skips checks if data was already loaded in destination")

	loadCmd.Flags().BoolVarP(&y, "yes", "y", false, "automatically answer yes for all questions")

	rootCmd.AddCommand(loadCmd)
}

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Start the data loading process",
	Run: func(cmd *cobra.Command, args []string) {
		if cmd.Flags().Changed("to-bundle-id") {
			setTo = true
		}

		loader, err := l.SetupLoader(configPath, connection, setTo, fromBundleId, toBundleId, force)
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
