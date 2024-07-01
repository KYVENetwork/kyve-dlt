package loader

import (
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
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
	sourceConfig collector.SourceConfig
	destination  destinations.Destination

	latestBundleId int64
}

type Config struct {
	ChannelSize    int
	CsvWorkerCount int
	SourceSchema   schema.DataSource
}

func NewLoader(loaderConfig Config, sourceConfig collector.SourceConfig, destination destinations.Destination) *Loader {
	return &Loader{
		config:       loaderConfig,
		sourceConfig: sourceConfig,
		destination:  destination,
	}
}
