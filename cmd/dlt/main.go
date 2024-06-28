package main

import (
	"KYVE-DLT/destinations"
	"KYVE-DLT/loader"
	"KYVE-DLT/loader/collector"
	"KYVE-DLT/schema"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"net/http"
	"time"

	_ "net/http/pprof"
)

func main() {

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	var rootCmd = &cobra.Command{
		Use:           "kyvedlt",
		Short:         "kyvedlt",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	rootCmd.AddCommand(&cobra.Command{
		Use:   "start",
		Short: "Start the sync",
		Run: func(cmd *cobra.Command, args []string) {
			go runPProf()
			log.Info().Msg("Starting Sync ...")
			startTime := time.Now().Unix()

			dest := destinations.NewBigQuery(destinations.BigQueryConfig{
				ProjectId:           "kyve-data-platform",
				DatasetId:           "airbyte_production",
				TableId:             "pool_5_dlt_benchmark",
				BigQueryWorkerCount: 1,
				BucketWorkerCount:   1,
			})
			_ = &dest

			postgresDest := destinations.NewPostgres(destinations.PostgresConfig{
				ConnectionUrl:       "",
				TableName:           "testing",
				PostgresWorkerCount: 2,
			})

			bundleConfig := collector.BundleFetcherConfig{
				PoolId:       5,
				FromBundleId: 0,
				ToBundleId:   1000,
				StepSize:     20,
				Endpoint:     "https://api.kyve.network",
			}

			loaderConfig := loader.Config{
				ChannelSize:    8,
				CsvWorkerCount: 4,
				SourceSchema:   schema.Tendermint{},
			}

			tendermintBigQueryLoader := loader.NewLoader(loaderConfig, bundleConfig, &postgresDest)
			tendermintBigQueryLoader.Start()

			fmt.Printf("Time: %d seconds\n", time.Now().Unix()-startTime)
		},
		PreRun: func(cmd *cobra.Command, args []string) {

		},
	})

	if err := rootCmd.Execute(); err != nil && err.Error() != "" {
		fmt.Println(err)
	}

}

func runPProf() {
	log.Info().Err(http.ListenAndServe("0.0.0.0:6061", nil))
}
