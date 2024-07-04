package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
)

func init() {
	destinationsCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	destinationsCmd.AddCommand(destinationsListCmd)

	rootCmd.AddCommand(destinationsCmd)
}

var destinationsCmd = &cobra.Command{
	Use:     "destinations",
	Short:   "Add or remove a destination or list all",
	Aliases: []string{"s"},
}

var destinationsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all specified destinations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			return
		}

		if len(config.Destinations) > 0 {
			columnOffset := 2
			var bigQueryDestinations []utils.Destination
			var postgresDestinations []utils.Destination
			for _, d := range config.Destinations {
				switch d.Type {
				case "big_query":
					bigQueryDestinations = append(bigQueryDestinations, d)
				case "postgres":
					postgresDestinations = append(postgresDestinations, d)
				default:
					logger.Error().Str("type", d.Type).Msg("destination type is not supported")
				}
			}

			// Calculate maximum length of each field
			maxNameLen, maxWorkerCountLen := len("Name"), len("Worker Count")
			for _, d := range config.Destinations {
				maxNameLen = max(maxNameLen, len(d.Name)) + columnOffset
				maxWorkerCountLen = max(maxWorkerCountLen, len(fmt.Sprint(d.WorkerCount))) + columnOffset
			}

			if len(bigQueryDestinations) > 0 {
				maxProjectIdLen, maxDatasetIdLen, maxTableIdLen, maxBucketWorkerLen := len("Project ID"), len("Dataset ID"), len("Table ID"), len("Worker Count")
				for _, d := range bigQueryDestinations {
					maxProjectIdLen = max(maxProjectIdLen, len(d.ProjectID)) + columnOffset
					maxDatasetIdLen = max(maxDatasetIdLen, len(d.DatasetID)) + columnOffset
					maxTableIdLen = max(maxTableIdLen, len(d.TableID)) + columnOffset
					maxBucketWorkerLen = max(maxBucketWorkerLen, len(fmt.Sprint(d.BucketWorkerCount))) + columnOffset
				}

				fmt.Println("====== BigQuery Destinations ======")
				fmt.Printf("\033[36m%-*s %-*s %-*s %-*s %-*s\033[0m\n", maxNameLen, "Name", maxProjectIdLen, "Project ID", maxDatasetIdLen, "Dataset ID", maxTableIdLen, "Table ID", maxWorkerCountLen, "Worker Count")
				for _, d := range bigQueryDestinations {
					fmt.Printf("%-*s %-*s %-*s %-*s %-*d\n", maxNameLen, d.Name, maxProjectIdLen, d.ProjectID, maxDatasetIdLen, d.DatasetID, maxTableIdLen, d.TableID, maxWorkerCountLen, d.WorkerCount)
				}
			}

			if len(postgresDestinations) > 0 {
				maxConnectionUrlLen, maxTableNameLen := len("Connection URL"), len("Table Name")
				for _, d := range postgresDestinations {
					maxConnectionUrlLen = max(maxConnectionUrlLen, len(d.ConnectionURL)) + columnOffset
					maxTableNameLen = max(maxTableNameLen, len(fmt.Sprint(d.TableName))) + columnOffset
				}

				fmt.Println("\n====== Postgres Destinations ======")
				fmt.Printf("\033[36m%-*s %-*s %-*s %-*s\033[0m\n", maxNameLen, "Name", maxConnectionUrlLen, "Connection URL", maxTableNameLen, "Table Name", maxWorkerCountLen, "Worker Count")
				for _, d := range postgresDestinations {
					fmt.Printf("%-*s %-*s %-*s %-*d\n", maxNameLen, d.Name, maxConnectionUrlLen, d.ConnectionURL, maxTableNameLen, d.TableName, maxWorkerCountLen, d.WorkerCount)
				}
			}
		} else {
			fmt.Println("No destinations defined.")
		}
	},
}
