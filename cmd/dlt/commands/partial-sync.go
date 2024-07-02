package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
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
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			return
		}

		source, destination, err := utils.GetConnectionDetails(config, connection)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to read connection")
			return
		}

		logger.Info().Int64("from_bundle_id", fromBundleId).Int64("to_bundle_id", toBundleId).Msg("Starting partial sync ...")
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
			ToBundleId:   toBundleId,
			StepSize:     int64(source.StepSize),
			Endpoint:     source.Endpoint,
			PartialSync:  true,
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
