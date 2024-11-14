package loader

import (
	"fmt"
	"math"
	"runtime/debug"
	"sync/atomic"

	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
)

func SetupLoader(configPath, connection string, setTo bool, from, to int64, force bool) (*Loader, error) {
	if !setTo {
		to = math.MaxInt64
	}

	config, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	utils.GLOBAL_MAX_RAM_GB = uint64(config.Loader.MaxRamGB)
	debug.SetMemoryLimit(int64(config.Loader.MaxRamGB * 1024 * 1024 * 1024))

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
			BucketName:          destination.BucketName,
			BigQueryWorkerCount: destination.WorkerCount,
			BucketWorkerCount:   destination.BucketWorkerCount,
		})
		dest = &bigQueryDest
	case "postgres":
		postgresDest := destinations.NewPostgres(destinations.PostgresConfig{
			ConnectionUrl:       destination.ConnectionURL,
			TableName:           destination.TableName,
			PostgresWorkerCount: destination.WorkerCount,
			RowInsertLimit:      destination.RowInsertLimit,
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
	case "height":
		sourceSchema = schema.Height{}
	case "tendermint_preprocessed":
		sourceSchema = schema.TendermintPreProcessed{}
	default:
		panic(fmt.Errorf("source schema not supported: %v", source.Schema))
	}

	loaderConfig := Config{
		ChannelSize:    config.Loader.ChannelSize,
		CsvWorkerCount: config.Loader.CSVWorkerCount,
		SourceSchema:   sourceSchema,
	}

	statusProperties := StatusProperties{
		syncId:                  uuid.New().String(),
		schemaType:              source.Schema,
		destinationType:         destination.Type,
		uncompressedBytesSynced: new(atomic.Int64),
		compressedBytesSynced:   new(atomic.Int64),
		bundlesSynced:           new(atomic.Int64),
	}

	return NewLoader(loaderConfig, sourceConfig, dest, connection, statusProperties), nil
}
