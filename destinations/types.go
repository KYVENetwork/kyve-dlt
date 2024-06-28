package destinations

import (
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"sync"
)

type Destination interface {
	StartProcess(schema schema.DataSource, csvChannel chan []schema.DataRow, waitGroup *sync.WaitGroup)
}
