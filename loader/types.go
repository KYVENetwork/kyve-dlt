package loader

import (
	"github.com/KYVENetwork/KYVE-DLT/destinations"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"sync"
	"sync/atomic"
	"time"
)

type BundlesBusItem struct {
	bundles []collector.Bundle

	status Status
}

type Loader struct {
	bundlesChannel     chan BundlesBusItem
	destinationChannel chan destinations.DestinationBusItem

	dataRowWaitGroup     sync.WaitGroup
	destinationWaitGroup sync.WaitGroup

	config         Config
	sourceConfig   collector.SourceConfig
	destination    destinations.Destination
	ConnectionName string

	latestBundleId *int64

	statusProperties StatusProperties
}

type StatusProperties struct {
	syncId                  string
	schemaType              string
	destinationType         string
	StartTime               time.Time
	uncompressedBytesSynced *atomic.Int64
	compressedBytesSynced   *atomic.Int64
	bundlesSynced           *atomic.Int64
}

type Config struct {
	ChannelSize    int
	CsvWorkerCount int
	SourceSchema   schema.DataSource
}

func NewLoader(loaderConfig Config, sourceConfig collector.SourceConfig, destination destinations.Destination, connectionName string, properties StatusProperties) *Loader {
	return &Loader{
		config:           loaderConfig,
		sourceConfig:     sourceConfig,
		destination:      destination,
		ConnectionName:   connectionName,
		statusProperties: properties,
	}
}
