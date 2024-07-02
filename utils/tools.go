package utils

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

func AwaitEnoughMemory(name string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	logger.Debug().
		Str("alloc", fmt.Sprintf("%v MiB", bToMb(m.Alloc))).
		Str("total-alloc", fmt.Sprintf("%v MiB", bToMb(m.TotalAlloc))).
		Str("sys", fmt.Sprintf("%v MiB", bToMb(m.Sys))).
		Str("num-gc", fmt.Sprintf("%v", m.NumGC)).
		Msg("SYSINFO")

	for m.Alloc > 200*1024*1024*1024 {
		runtime.ReadMemStats(&m)

		logger.Debug().Msg(
			fmt.Sprintf("SYSINFO: Alloc = %v MiB", bToMb(m.Alloc)) +
				fmt.Sprintf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc)) +
				fmt.Sprintf("\tSys = %v MiB", bToMb(m.Sys)) +
				fmt.Sprintf("\tNumGC = %v\n", m.NumGC) +
				fmt.Sprintf("(%s) Memory limit reached (%d MiB): Waiting 10 seconds ...", name, m.Alloc),
		)

		logger.Debug().
			Str("alloc", fmt.Sprintf("%v MiB", bToMb(m.Alloc))).
			Str("total-alloc", fmt.Sprintf("%v MiB", bToMb(m.TotalAlloc))).
			Str("sys", fmt.Sprintf("%v MiB", bToMb(m.Sys))).
			Str("num-gc", fmt.Sprintf("%v", m.NumGC)).
			Msg(fmt.Sprintf("SYSINFO: (%s) Memory limit reached (%d MiB): Waiting 10 seconds ...", name, m.Alloc))

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
