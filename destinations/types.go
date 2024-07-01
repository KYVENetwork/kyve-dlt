package destinations

import (
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"sync"
)

var (
	logger = utils.DltLogger("destinations")
)

type Destination interface {
	Close()
	GetLatestBundleId() string
	Initialize(schema schema.DataSource, csvChannel chan []schema.DataRow)
	StartProcess(waitGroup *sync.WaitGroup)
}
