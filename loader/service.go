package loader

import (
	"context"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"strconv"
	"time"
)

var (
	logger = utils.DltLogger("loader")
)

func (loader *Loader) Start(ctx context.Context, y bool, sync bool) {
	logger.Debug().Msg(fmt.Sprintf("BundleConfig: %#v", loader.sourceConfig))
	logger.Debug().Msg(fmt.Sprintf("ConcurrencyConfig: %#v", loader.config))

	loader.bundlesChannel = make(chan BundlesBusItem, loader.config.ChannelSize)
	loader.destinationChannel = make(chan destinations.DestinationBusItem, loader.config.ChannelSize)

	loader.destination.Initialize(loader.config.SourceSchema, loader.destinationChannel)

	loader.latestBundleId = loader.destination.GetLatestBundleId()
	if loader.latestBundleId != nil {
		logger.Warn().Str("connection", loader.ConnectionName).Int64("highest_bundle_id", *loader.latestBundleId).Msg("found loaded data in destination")
		if !loader.sourceConfig.Force {
			loader.sourceConfig.FromBundleId = *loader.latestBundleId + 1
			if !sync {
				logger.Info().Str("connection", loader.ConnectionName).Int64("id", loader.sourceConfig.FromBundleId).
					Msg("set new from_bundle_id - this step can be skipped with --force")
			}
		}
	} else {
		logger.Debug().Str("connection", loader.ConnectionName).Msg("detected initial sync")
	}

	// PartialSync is enabled when --to-bundle-id is set
	if loader.sourceConfig.PartialSync && !loader.sourceConfig.Force {
		if loader.sourceConfig.FromBundleId > loader.sourceConfig.ToBundleId {
			logger.Error().Int64("from", loader.sourceConfig.FromBundleId).
				Str("connection", loader.ConnectionName).
				Int64("to", loader.sourceConfig.ToBundleId).
				Msg("from_bundle_id > to_bundle_id - this step can be skipped with --force")
			return
		}
	}

	if !y {
		if !loader.sourceConfig.PartialSync {
			if !utils.PromptConfirm(fmt.Sprintf("\u001B[36m[DLT]\u001B[0m Should data from bundle_id %d be loaded until all bundles are synced?\n\u001B[36m[y/N]\u001B[0m: ", loader.sourceConfig.FromBundleId)) {
				logger.Error().Msg("aborted")
				return
			}
		} else {
			if !utils.PromptConfirm(fmt.Sprintf("\u001B[36m[DLT]\u001B[0m Should data from bundle_id %d to %d be loaded?\n\u001B[36m[y/N]\u001B[0m: ", loader.sourceConfig.FromBundleId, loader.sourceConfig.ToBundleId)) {
				logger.Error().Msg("aborted")
				return
			}
		}
	}

	loaderConfigStatus := utils.LoaderConfigProperties{
		SyncId:         loader.statusProperties.syncId,
		Schema:         loader.statusProperties.schemaType,
		Destination:    loader.statusProperties.destinationType,
		ChannelSize:    loader.config.ChannelSize,
		CSVWorkerCount: loader.config.CsvWorkerCount,
		MaxRamGB:       utils.GLOBAL_MAX_RAM_GB,
		PoolId:         loader.sourceConfig.PoolId,
		Endpoint:       loader.sourceConfig.Endpoint,
		FromBundleId:   loader.sourceConfig.FromBundleId,
		ToBundleId:     loader.sourceConfig.ToBundleId,
	}
	loader.statusProperties.StartTime = time.Now()
	utils.TrackSyncStarted(loaderConfigStatus)

	//Fetches bundles from api.kyve.network
	go loader.bundlesCollector(ctx)

	// Downloads bundles from Arweave and converts preprocesses them
	loader.dataRowWaitGroup.Add(loader.config.CsvWorkerCount)
	for i := 1; i <= loader.config.CsvWorkerCount; i++ {
		go loader.dataRowWorker(fmt.Sprintf("csv-%d", i))
	}

	loader.destination.StartProcess(&loader.destinationWaitGroup)

	loader.dataRowWaitGroup.Wait()
	close(loader.destinationChannel)

	loader.destinationWaitGroup.Wait()

	loader.destination.Close()

	utils.TrackSyncFinished(loaderConfigStatus, utils.SyncFinishedProperties{
		Duration:                time.Now().Unix() - loader.statusProperties.StartTime.Unix(),
		CompressedBytesSynced:   loader.statusProperties.compressedBytesSynced.Load(),
		UncompressedBytesSynced: loader.statusProperties.uncompressedBytesSynced.Load(),
		BundlesSynced:           loader.statusProperties.bundlesSynced.Load(),
	})
}

func (loader *Loader) bundlesCollector(ctx context.Context) {
	defer close(loader.bundlesChannel)

	fetcher, err := collector.NewSource(loader.sourceConfig)

	if err != nil {
		panic(err)
	}

	offset := loader.sourceConfig.FromBundleId
	logger.Debug().Str("connection", loader.ConnectionName).Int64("bundle_id", offset).Msg("setting offset")

	fetcher.FetchBundles(ctx, offset, loader.ConnectionName, func(bundles []collector.Bundle, err error) {
		if err != nil {
			logger.Error().Msg(fmt.Sprintf("error fetching bundles: %v", err))
			logger.Info().Msg("waiting...")
			utils.PrometheusSyncStepFailedRetry.WithLabelValues(loader.ConnectionName).Inc()
			time.Sleep(5 * time.Second)
		} else {
			if len(bundles) > 0 {
				fromBundleId, _ := strconv.ParseUint(bundles[0].Id, 10, 64)
				toBundleId, _ := strconv.ParseUint(bundles[len(bundles)-1].Id, 10, 64)

				logger.Info().
					Str("connection", loader.ConnectionName).
					Int64("from", int64(fromBundleId)).
					Int64("to", int64(toBundleId)).
					Int("amount", len(bundles)).
					Msg("fetched")

				loader.bundlesChannel <- BundlesBusItem{
					bundles: bundles,
					status: Status{
						FromBundleId: int64(fromBundleId),
						ToBundleId:   int64(toBundleId),
						FromKey:      bundles[0].FromKey,
						ToKey:        bundles[len(bundles)-1].ToKey,
						ExtractedAt:  time.Now().Format(time.RFC3339Nano),
					},
				}
			}
		}
	})
}

func (loader *Loader) dataRowWorker(name string) {
	defer loader.dataRowWaitGroup.Done()

	for {
		item, ok := <-loader.bundlesChannel
		if !ok {
			logger.Info().
				Str("connection", loader.ConnectionName).
				Str("worker-id", name).
				Msg("finished")
			return
		}

		utils.AwaitEnoughMemory(name)

		items := make([]schema.DataRow, 0)

		totalUncompressedSize := int64(0)
		totalCompressedSize := int64(0)

		for _, k := range item.bundles {
			utils.TryWithExponentialBackoff(func() error {
				result, err := loader.config.SourceSchema.DownloadAndConvertBundle(k, schema.ExtraData{
					Name:        name,
					ExtractedAt: item.status.ExtractedAt,
				})
				if err != nil {
					return err
				}
				items = append(items, result.Data...)
				totalUncompressedSize += result.UncompressedSize
				totalCompressedSize += result.CompressedSize
				return nil
			}, func(err error) {
				logger.Error().Str("connection", loader.ConnectionName).Msg(fmt.Sprintf("(%s) error: %s \nRetry in 5 seconds.\n", name, err.Error()))
				utils.PrometheusSyncStepFailedRetry.WithLabelValues(loader.ConnectionName).Inc()
			})
		}

		loader.destinationChannel <- destinations.DestinationBusItem{
			Data:         items,
			FromBundleId: item.status.FromBundleId,
			ToBundleId:   item.status.ToBundleId,
		}

		utils.PrometheusBundlesSynced.WithLabelValues(loader.ConnectionName).Add(float64(item.status.ToBundleId - item.status.FromBundleId + 1))
		utils.PrometheusCurrentBundleHeight.WithLabelValues(loader.ConnectionName).Set(float64(item.status.ToBundleId))

		loader.statusProperties.compressedBytesSynced.Add(totalCompressedSize)
		loader.statusProperties.uncompressedBytesSynced.Add(totalUncompressedSize)
		loader.statusProperties.bundlesSynced.Add(int64(len(item.bundles)))

		utils.PrometheusCompressedBytesSynced.WithLabelValues(loader.ConnectionName).Add(float64(totalCompressedSize))
		utils.PrometheusUncompressedBytesSynced.WithLabelValues(loader.ConnectionName).Add(float64(totalUncompressedSize))

		logger.Info().
			Str("worker-id", name).
			Str("connection", loader.ConnectionName).
			Int64("fromBundleId", item.status.FromBundleId).
			Str("toKey", item.status.ToKey).
			Int64("toBundleId", item.status.ToBundleId).
			Int("bundles", len(item.bundles)).
			Int64("size", totalCompressedSize).
			Msg("converted")
	}

}
