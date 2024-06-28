package loader

import (
	"KYVE-DLT/destinations"
	"KYVE-DLT/loader/collector"
	"KYVE-DLT/schema"
	"sync"
)

type BundlesBusItem struct {
	bundles []collector.Bundle

	status Status
}

type Loader struct {
	bundlesChannel chan BundlesBusItem
	dataRowChannel chan []schema.DataRow

	dataRowWaitGroup     sync.WaitGroup
	destinationWaitGroup sync.WaitGroup

	config       Config
	bundleConfig collector.BundleFetcherConfig
	destination  destinations.Destination
}

type Config struct {
	ChannelSize    int
	CsvWorkerCount int
	SourceSchema   schema.DataSource
}

func NewLoader(loaderConfig Config, bundleConfig collector.BundleFetcherConfig, destination destinations.Destination) *Loader {
	return &Loader{
		config:       loaderConfig,
		bundleConfig: bundleConfig,
		destination:  destination,
	}
}
