package destinations

import (
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"sync"
)

type Destination interface {
	Close()
	GetLatestBundleId() *int64
	Initialize(schema schema.DataSource, destinationChannel chan DestinationBusItem)
	StartProcess(waitGroup *sync.WaitGroup)
}

type DestinationBusItem struct {
	Data         []schema.DataRow
	FromBundleId int64
	ToBundleId   int64
}
