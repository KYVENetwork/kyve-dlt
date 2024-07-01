package loader

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"strconv"
	"time"
)

var (
	logger = utils.DltLogger("CSV")
)

func (loader *Loader) Start() {

	logger.Debug().Msg(fmt.Sprintf("BundleConfig: %#v", loader.sourceConfig))
	logger.Debug().Msg(fmt.Sprintf("ConcurrencyConfig: %#v", loader.config))

	loader.bundlesChannel = make(chan BundlesBusItem, loader.config.ChannelSize)
	loader.dataRowChannel = make(chan []schema.DataRow, loader.config.ChannelSize)

	loader.destination.Initialize(loader.config.SourceSchema, loader.dataRowChannel)

	latestBundleId := loader.destination.GetLatestBundleId()

	//Fetches bundles from api.kyve.network
	go loader.bundlesCollector(latestBundleId)

	// Downloads bundles from Arweave and converts preprocesses them
	loader.dataRowWaitGroup.Add(loader.config.CsvWorkerCount)
	for i := 1; i <= loader.config.CsvWorkerCount; i++ {
		go loader.dataRowWorker(fmt.Sprintf("CSV - %d", i))
	}

	loader.destination.StartProcess(&loader.destinationWaitGroup)

	loader.dataRowWaitGroup.Wait()
	close(loader.dataRowChannel)

	loader.destination.Close()

	loader.destinationWaitGroup.Wait()
}

func (loader *Loader) bundlesCollector(latestBundleId int64) {
	defer close(loader.bundlesChannel)

	fetcher, err := collector.NewSource(loader.sourceConfig)

	if err != nil {
		panic(err)
	}

	fetcher.FetchBundles(latestBundleId, func(bundles []collector.Bundle, err error) {
		if err != nil {
			logger.Error().Msg(fmt.Sprintf("Error fetching bundles: %v\nWaiting ... ", err))
			time.Sleep(5 * time.Second)
		} else {
			fromBundleId, _ := strconv.ParseUint(bundles[0].Id, 10, 64)
			toBundleId, _ := strconv.ParseUint(bundles[len(bundles)-1].Id, 10, 64)
			loader.bundlesChannel <- BundlesBusItem{
				bundles: bundles,
				status: Status{
					FromBundleId: int64(fromBundleId),
					ToBundleId:   int64(toBundleId),
					FromKey:      bundles[0].FromKey,
					ToKey:        bundles[len(bundles)-1].ToKey,
					DataSize:     0,
				},
			}
		}
	})
}

func (loader *Loader) dataRowWorker(name string) {
	defer loader.dataRowWaitGroup.Done()

	for {
		item, ok := <-loader.bundlesChannel
		if !ok {
			logger.Info().Msg(fmt.Sprintf("(%s) Finished", name))
			return
		}

		utils.AwaitEnoughMemory(name)

		items := make([]schema.DataRow, 0)
		for _, k := range item.bundles {

			utils.TryWithExponentialBackoff(func() error {
				newRows, err := loader.config.SourceSchema.DownloadAndConvertBundle(k)
				if err != nil {
					return err
				}
				items = append(items, newRows...)
				return nil
			}, func(err error) {
				logger.Error().Msg(fmt.Sprintf("(%s) error: %s \nRetry in 5 seconds.\n", name, err.Error()))
			})
		}

		loader.dataRowChannel <- items

		logger.Info().
			Str("fromKey", item.status.FromKey).
			Str("toKey", item.status.ToKey).
			Int64("toBundleId", item.status.ToBundleId).
			Int("bundles", len(item.bundles)).
			Msg("converted")
	}

}
