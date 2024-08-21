package commands

import (
	"context"
	"fmt"
	l "github.com/KYVENetwork/KYVE-DLT/loader"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/go-co-op/gocron/v2"
	"github.com/spf13/cobra"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func init() {
	syncCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	syncCmd.Flags().StringVarP(&connectionName, "connections", "c", "", "name of the connections to sync (comma separated)")

	syncCmd.Flags().BoolVarP(&all, "all", "a", false, "sync all specified connections")

	syncCmd.Flags().BoolVarP(&force, "force", "f", false, "skips checks if data was already loaded in destination")

	rootCmd.AddCommand(syncCmd)
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run a supervised incremental sync",
	Run: func(cmd *cobra.Command, args []string) {
		if connectionName == "" && !all {
			logger.Error().Msg("either --connections or --all is required")
			return
		}

		config, err := utils.LoadConfig(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		logger.Debug().Int64("from_bundle_id", fromBundleId).Msg("setting up supervised sync")

		var connections []utils.Connection
		allConnections, err := utils.GetAllConnections(config)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to get all connections")
			return
		}

		if all {
			connections = *allConnections
		} else {
			connectionsSlice := strings.Split(connectionName, ",")
		ConnectionSelect:
			for _, c := range connectionsSlice {
				for _, conn := range *allConnections {
					if strings.TrimSpace(c) == conn.Name {
						connections = append(connections, conn)
						continue ConnectionSelect
					}
				}
				logger.Error().Msg(fmt.Sprintf("connectionName %v not found", c))
				return
			}
		}

		// Required for graceful shutdown
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		shutdownChannel := make(chan os.Signal, 1)
		signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

		running := false
		sigCount := 0

		cronScheduler, err := gocron.NewScheduler()
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to create cron scheduler")
			return
		}

		var oneSyncAtATime sync.Mutex
		// Set up loader and Cron job for each connection
		for _, c := range connections {
			// TODO: Improve loader handling to prevent future concurrency issues. Use channel structure instead.
			loader, err := l.SetupLoader(configPath, c.Name, false, fromBundleId, math.MaxInt64, force)
			if err != nil {
				logger.Error().Str("connectionName", c.Name).Str("err", err.Error()).Msg("failed to set up loader")
				return
			}

			logger.Info().Str("connectionName", c.Name).Str("schedule", c.Cron).Msg(fmt.Sprintf("adding connection task to cron scheduler"))

			// Cron scheduler setup
			_, err = cronScheduler.NewJob(
				// Register a Cron job for connection with the config's crontab
				gocron.CronJob(
					c.Cron, false,
				),
				// Add the loading process as Cron task to be executed in the crontab schedule
				gocron.NewTask(
					func() {
						running = true
						startTime := time.Now().Unix()

						// Lock to ensure that only one loading process is running at a time
						oneSyncAtATime.Lock()

						logger.Info().Str("connection", loader.ConnectionName).Msg("starting loading process")

						loader.Start(ctx, true, true)

						logger.Info().Msg(fmt.Sprintf("Finished sync for %v! Took %d seconds", loader.ConnectionName, time.Now().Unix()-startTime))
						oneSyncAtATime.Unlock()
						running = false

						// Exit if signal was received during loading process
						if sigCount >= 1 {
							os.Exit(1)
						}
					},
				),
			)
			if err != nil {
				logger.Error().Str("connectionName", loader.ConnectionName).Str("err", err.Error()).Msg("failed to set up cronjob")
				return
			}
		}
		// This will start the Cron scheduler to execute the specified tasks as goroutines
		cronScheduler.Start()

		// Handle shutdown
		go func() {
			for {
				<-shutdownChannel
				if running {
					sigCount++
					if sigCount == 1 {
						// First signal, attempt graceful shutdown
						cancel()
						logger.Info().Msg("Exiting...")
						logger.Warn().Msg("This can take some time, please wait until dlt exited!")
					} else if sigCount == 2 {
						// Second signal, force exit
						logger.Warn().Msg("Received second signal, forcing exit...")
						os.Exit(1)
					}
				} else {
					os.Exit(1)
				}
			}
		}()

		select {}
	},
}
