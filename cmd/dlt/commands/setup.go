package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"math"
)

func setupLoader(configPath string, setTo bool, from, to int64, force bool) (*loader.Loader, error) {
	if !setTo {
		to = math.MaxInt64
	}

	config, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	source, destination, err := utils.GetConnectionDetails(config, connection)
	if err != nil {
		return nil, fmt.Errorf("failed to read connection: %v", err)
	}

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
		FromBundleId: from,
		ToBundleId:   to,
		BatchSize:    int64(source.BatchSize),
		Endpoint:     source.Endpoint,
		PartialSync:  setTo,
		Force:        force,
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

	return loader.NewLoader(loaderConfig, sourceConfig, dest), nil
}
