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

var SegmentKey = "cwVoYw4i6hMDgd7Zlnna0MTM13FGNgcM"

var (
	client = analytics.New(SegmentKey)
	OptOut = false
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
			Name:    "dlt",
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

func getUserId() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	dltDir := filepath.Join(home, ".kyve-dlt")
	if _, err = os.Stat(dltDir); os.IsNotExist(err) {
		if err := os.Mkdir(dltDir, 0o755); err != nil {
			return "", err
		}
	}

	userId := uuid.New().String()

	idFile := filepath.Join(dltDir, "id")
	if _, err = os.Stat(idFile); os.IsNotExist(err) {
		if err := os.WriteFile(idFile, []byte(userId), 0o755); err != nil {
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

type LoaderConfigProperties struct {
	SyncId         string
	Schema         string
	Destination    string
	ChannelSize    int
	CSVWorkerCount int
	MaxRamGB       uint64
	PoolId         int64
	Endpoint       string
	FromBundleId   int64
	ToBundleId     int64
}

func TrackSyncStarted(properties LoaderConfigProperties) {
	trackEvent("Sync Started", buildLoaderProperties(properties))
}

type SyncFinishedProperties struct {
	SyncId                  string
	Duration                int64
	CompressedBytesSynced   int64
	UncompressedBytesSynced int64
	BundlesSynced           int64
}

func TrackSyncFinished(loaderConfigProperties LoaderConfigProperties, finishedProperties SyncFinishedProperties) {
	p := buildLoaderProperties(loaderConfigProperties).
		Set("duration", finishedProperties.Duration).
		Set("compressed_bytes_synced", finishedProperties.CompressedBytesSynced).
		Set("uncompressed_bytes_synced", finishedProperties.UncompressedBytesSynced).
		Set("bundles_synced", finishedProperties.BundlesSynced)

	trackEvent("Sync Finished", p)

	// flush all events
	err := client.Close()
	_ = err
}

func trackEvent(name string, properties analytics.Properties) {
	if OptOut {
		return
	}

	userId, err := getUserId()
	if err != nil {
		return
	}

	err = client.Enqueue(analytics.Track{
		UserId:     userId,
		Event:      name,
		Properties: properties,
		Context:    getContext(),
	})
}

func buildLoaderProperties(properties LoaderConfigProperties) analytics.Properties {
	// sanitize free-form input
	if !strings.HasSuffix(properties.Endpoint, "kyve.network") &&
		!strings.HasSuffix(properties.Endpoint, "kyve.network/") {
		properties.Endpoint = "private"
	}

	return analytics.NewProperties().
		Set("sync_id", properties.SyncId).
		Set("schema", properties.Schema).
		Set("destination", properties.Destination).
		Set("channel_size", properties.ChannelSize).
		Set("csv_worker_count", properties.CSVWorkerCount).
		Set("max_ram_gb", properties.MaxRamGB).
		Set("pool_id", properties.MaxRamGB).
		Set("endpoint", properties.Endpoint).
		Set("from_bundle_id", properties.FromBundleId).
		Set("to_bundle_id", properties.ToBundleId)
}
