package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"time"

	_ "net/http/pprof"
)

func init() {
	partialSyncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

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
		if err := utils.LoadConfig(configPath); err != nil {
			return
		}

		logger.Info().Int64("from_bundle_id", fromBundleId).Int64("to_bundle_id", toBundleId).Msg("Starting partial sync ...")
		startTime := time.Now().Unix()

		var dest destinations.Destination
		switch viper.GetString("destination.type") {
		case "big_query":
			bigQueryDest := destinations.NewBigQuery(destinations.BigQueryConfig{
				ProjectId:           viper.GetString("destination.big_query.project_id"),
				DatasetId:           viper.GetString("destination.big_query.dataset_id"),
				TableId:             viper.GetString("destination.big_query.table_id"),
				BigQueryWorkerCount: viper.GetInt("destination.big_query.big_query_worker_count"),
				BucketWorkerCount:   viper.GetInt("destination.big_query.bucket_worker_count"),
			})
			dest = &bigQueryDest
		case "postgres":
			postgresDest := destinations.NewPostgres(destinations.PostgresConfig{
				ConnectionUrl:       viper.GetString("destination.postgres.connection_url"),
				TableName:           viper.GetString("destination.postgres.table_name"),
				PostgresWorkerCount: viper.GetInt("destination.postgres.worker_count"),
			})
			dest = &postgresDest
		default:
			panic(fmt.Errorf("destination type not supported: %v", viper.GetString("destination.type")))
		}

		sourceConfig := collector.SourceConfig{
			PoolId:       viper.GetInt64("source.pool_id"),
			FromBundleId: fromBundleId,
			ToBundleId:   toBundleId,
			StepSize:     viper.GetInt64("source.step_size"),
			Endpoint:     viper.GetString("source.endpoint"),
			PartialSync:  true,
		}

		var sourceSchema schema.DataSource
		switch viper.GetString("source.schema") {
		case "base":
			sourceSchema = schema.Base{}
		case "tendermint":
			sourceSchema = schema.Tendermint{}
		case "tendermint_preprocessed":
			sourceSchema = schema.TendermintPreProcessed{}
		default:
			panic(fmt.Errorf("source schema not supported: %v", viper.GetString("source.schema")))
		}

		loaderConfig := loader.Config{
			ChannelSize:    viper.GetInt("loader.channel_size"),
			CsvWorkerCount: viper.GetInt("loader.csv_worker_count"),
			SourceSchema:   sourceSchema,
		}

		if !y {
			answer := ""

			fmt.Printf("\u001B[36m[DLT]\u001B[0m Should data from bundle_id %d to %d be partially loaded into %v?\n[y/N]: ", fromBundleId, toBundleId, viper.GetString("destination.type"))

			if _, err := fmt.Scan(&answer); err != nil {
				logger.Error().Str("err", err.Error()).Msg("failed to read user input")
				return
			}

			if strings.ToLower(answer) != "y" {
				logger.Info().Msg("aborted")
				return
			}
		}

		loader.NewLoader(loaderConfig, sourceConfig, dest).Start()

		logger.Info().Msg(fmt.Sprintf("Time: %d seconds", time.Now().Unix()-startTime))
	},
	PreRun: func(cmd *cobra.Command, args []string) {

	},
}
