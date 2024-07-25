package utils

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/analytics-go"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

var (
	syncId = uuid.New().String()
	client = analytics.New(SegmentKey)
)

func getContext() *analytics.Context {
	version := "local"
	build, _ := debug.ReadBuildInfo()

	if strings.TrimSpace(build.Main.Version) != "" {
		version = strings.TrimSpace(build.Main.Version)
	}

	timezone, _ := time.Now().Zone()
	locale := os.Getenv("LANG")

	return &analytics.Context{
		App: analytics.AppInfo{
			Name:    "ksync",
			Version: version,
		},
		Location: analytics.LocationInfo{},
		OS: analytics.OSInfo{
			Name: fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH),
		},
		Locale:   locale,
		Timezone: timezone,
	}
}

func getUserId(configPath string) (string, error) {
	var idFile string
	if configPath == "" {
		idFile = filepath.Join(configPath, "id")
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}

		dltDir := filepath.Join(home, ".kyve-dlt")
		if _, err = os.Stat(dltDir); os.IsNotExist(err) {
			if err = os.Mkdir(dltDir, 0o755); err != nil {
				return "", err
			}
		}
		idFile = filepath.Join(dltDir, "id")
	}

	var userId string
	if _, err := os.Stat(idFile); os.IsNotExist(err) {
		userId = uuid.New().String()
		if err = os.WriteFile(idFile, []byte(userId), 0o755); err != nil {
			return "", err
		}
	} else {
		data, err := os.ReadFile(idFile)
		if err != nil {
			return "", err
		}
		userId = string(data)
	}

	return userId, nil
}

func TrackLoadCompletedEvent(configPath string, optOut bool, poolId int64) {
	if optOut {
		return
	}

	userId, err := getUserId(configPath)
	if err != nil {
		return
	}

	err = client.Enqueue(analytics.Track{
		UserId: userId,
		Event:  LoadCompleted,
		Properties: analytics.NewProperties().
			Set("pool_id", poolId),
		Context: getContext(),
	})

	err = client.Close()
	_ = err
}

func TrackLoadStartedEvent(configPath string, optOut bool, poolId int64) {
	if optOut {
		return
	}

	userId, err := getUserId(configPath)
	if err != nil {
		return
	}

	err = client.Enqueue(analytics.Track{
		UserId: userId,
		Event:  LoadStarted,
		Properties: analytics.NewProperties().
			Set("pool_id", poolId),
		Context: getContext(),
	})

	err = client.Close()
	_ = err
}

func TrackSyncStartedEvent(configPath string, optOut bool, connectionCount int) {
	if optOut {
		return
	}

	userId, err := getUserId(configPath)
	if err != nil {
		return
	}

	err = client.Enqueue(analytics.Track{
		UserId: userId,
		Event:  SyncStarted,
		Properties: analytics.NewProperties().
			Set("connection_count", connectionCount),
		Context: getContext(),
	})

	err = client.Close()
	_ = err
}
