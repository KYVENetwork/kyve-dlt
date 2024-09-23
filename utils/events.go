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
