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
	"math"
	"strings"
	"time"

	_ "net/http/pprof"
)

func init() {
	syncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	syncCmd.Flags().Int64Var(&fromBundleId, "from-bundle-id", 0, "start bundle-id of the initial sync process")

	syncCmd.Flags().BoolVarP(&y, "yes", "y", false, "automatically answer yes for all questions")

	rootCmd.AddCommand(syncCmd)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Start the incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		if err := utils.LoadConfig(configPath); err != nil {
			return
		}

		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("Starting incremental sync ...")
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
			ToBundleId:   math.MaxInt64,
			StepSize:     viper.GetInt64("source.step_size"),
			Endpoint:     viper.GetString("source.endpoint"),
			PartialSync:  false,
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

			fmt.Printf("\u001B[36m[DLT]\u001B[0m Should data from bundle_id %d be loaded into %d [y/N]: ", fromBundleId, viper.GetString("destination.type"))

			if _, err := fmt.Scan(&answer); err != nil {
				logger.Error().Str("err", err.Error()).Msg("failed to read user input")
				return
			}

			if strings.ToLower(answer) != "y" {
				logger.Error().Msg("aborted")
				return
			}
		}

		loader.NewLoader(loaderConfig, sourceConfig, dest).Start()

		logger.Info().Msg(fmt.Sprintf("Time: %d seconds", time.Now().Unix()-startTime))
	},
	PreRun: func(cmd *cobra.Command, args []string) {

	},
}
