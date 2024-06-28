package tools

import (
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"math/rand"
	"os"
	"runtime"
	"time"
	_ "unsafe"
)

func AwaitEnoughMemory(name string, verbose bool) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if verbose {
		fmt.Printf("SYSINFO: Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
	}

	for m.Alloc > 200*1024*1024*1024 {
		runtime.ReadMemStats(&m)

		fmt.Printf("SYSINFO: Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
		fmt.Printf("(%s) Memory limit reached (%d MiB): Waiting 10 seconds ... \n", name, m.Alloc)

		runtime.GC()
		time.Sleep((10 + time.Duration(rand.Intn(10))) * time.Second)
	}

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func DltLogger(moduleName string) zerolog.Logger {
	writer := io.MultiWriter(os.Stdout)
	customConsoleWriter := zerolog.ConsoleWriter{Out: writer}
	customConsoleWriter.FormatCaller = func(i interface{}) string {
		return "\x1b[36m[DLT]\x1b[0m"
	}

	logger := zerolog.New(customConsoleWriter).With().Str("module", moduleName).Timestamp().Logger()
	return logger
}

func TryWithExponentialBackoff(try func() error, onError func(error)) {
	importErr := try()
	var timeout int64 = 1
	for importErr != nil {
		onError(importErr)
		time.Sleep(time.Second * time.Duration(timeout))
		importErr = try()
		timeout *= 2
	}
}
