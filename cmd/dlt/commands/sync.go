package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
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
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			return
		}

		source, destination, err := utils.GetConnectionDetails(config, connection)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to read connection")
			return
		}

		logger.Info().Int64("from_bundle_id", fromBundleId).Msg("Starting incremental sync ...")
		startTime := time.Now().Unix()

		var dest destinations.Destination
		switch destination.Type {
		case "big_query":
			bigQueryDest := destinations.NewBigQuery(destinations.BigQueryConfig{
				ProjectId:           destination.ProjectID,
				DatasetId:           destination.DatasetID,
				TableId:             destination.TableID,
				BigQueryWorkerCount: destination.WorkerCount,
				BucketWorkerCount:   destination.BucketWorkerCount,
			})
			dest = &bigQueryDest
		case "postgres":
			postgresDest := destinations.NewPostgres(destinations.PostgresConfig{
				ConnectionUrl:       destination.ConnectionURL,
				TableName:           destination.TableName,
				PostgresWorkerCount: destination.WorkerCount,
			})
			dest = &postgresDest
		default:
			panic(fmt.Errorf("destination type not supported: %v", destination.Type))
		}

		sourceConfig := collector.SourceConfig{
			PoolId:       int64(source.PoolID),
			FromBundleId: fromBundleId,
			ToBundleId:   math.MaxInt64,
			StepSize:     int64(source.StepSize),
			Endpoint:     source.Endpoint,
			PartialSync:  false,
		}

		var sourceSchema schema.DataSource
		switch source.Schema {
		case "base":
			sourceSchema = schema.Base{}
		case "tendermint":
			sourceSchema = schema.Tendermint{}
		case "tendermint_preprocessed":
			sourceSchema = schema.TendermintPreProcessed{}
		default:
			panic(fmt.Errorf("source schema not supported: %v", source.Schema))
		}

		loaderConfig := loader.Config{
			ChannelSize:    config.Loader.ChannelSize,
			CsvWorkerCount: config.Loader.CSVWorkerCount,
			SourceSchema:   sourceSchema,
		}

		loader.NewLoader(loaderConfig, sourceConfig, dest).Start(y)

		logger.Info().Msg(fmt.Sprintf("Time: %d seconds", time.Now().Unix()-startTime))
	},
	PreRun: func(cmd *cobra.Command, args []string) {

	},
}
