package commands

import (
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func init() {
	destinationsCmd.Flags().StringVar(&configPath, "config", utils.DefaultHomePath, "set custom config path")

	destinationsCmd.AddCommand(destinationsAddCmd)
	destinationsCmd.AddCommand(destinationsListCmd)
	destinationsCmd.AddCommand(destinationsRemoveCmd)

	rootCmd.AddCommand(destinationsCmd)
}

var destinationsCmd = &cobra.Command{
	Use:     "destinations",
	Short:   "Add or remove a destination or list all",
	Aliases: []string{"d"},
}

var destinationsAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new destination",
	Run: func(cmd *cobra.Command, args []string) {
		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		destinationType := utils.PromptDestinationDropdown("\033[36mSelect destination: \033[0m", []string{"big_query", "postgres"})

		var newDestination yaml.Node
		switch destinationType {
		case "big_query":
			newDestination = yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					{Kind: yaml.ScalarNode, Value: "name"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Destination name: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "type"},
					{Kind: yaml.ScalarNode, Value: "big_query"},
					{Kind: yaml.ScalarNode, Value: "project_id"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Project ID: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "dataset_id"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Dataset ID: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "table_id"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Table ID: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "worker_count"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Worker count (default 2): \033[0m", "2")},
					{Kind: yaml.ScalarNode, Value: "bucket_worker_count"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Bucket Worker count (default 2): \033[0m", "2")},
				},
			}
		case "postgres":
			newDestination = yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					{Kind: yaml.ScalarNode, Value: "name"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Destination name: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "type"},
					{Kind: yaml.ScalarNode, Value: "postgres"},
					{Kind: yaml.ScalarNode, Value: "connection_url"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Connection URL: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "table_name"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInput("\033[36mEnter Table name: \033[0m")},
					{Kind: yaml.ScalarNode, Value: "worker_count"},
					{Kind: yaml.ScalarNode, Value: utils.PromptInputWithDefault("\033[36mEnter Worker count (default 4): \033[0m", "4")},
				},
			}
		}

		// Find the destinations node
		var destinationsNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "destinations" {
				destinationsNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Append the new destinations
		if destinationsNode != nil {
			destinationsNode.Content = append(destinationsNode.Content, &newDestination)
		} else {
			configNode.Content[0].Content = append(configNode.Content[0].Content, &yaml.Node{
				Kind:    yaml.ScalarNode,
				Value:   "destinations",
				Tag:     "!!seq",
				Content: []*yaml.Node{&newDestination},
			})
		}

		if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
			logger.Error().Str("err", err.Error()).Msg("error saving config")
			return
		}

		logger.Info().Msg("Destination added successfully!")
	},
}

var destinationsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all specified destinations",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := utils.LoadConfig(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
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

var destinationsRemoveCmd = &cobra.Command{
	Use:   "remove [destination name]",
	Short: "Remove a destination by name",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configNode, err := utils.LoadConfigWithComments(configPath)
		if err != nil {
			logger.Error().Str("err", err.Error()).Msg("failed to load config")
			return
		}

		destinationName := args[0]

		// Find the sources node
		var destinationsNode *yaml.Node
		for i, node := range configNode.Content[0].Content {
			if node.Value == "destinations" {
				destinationsNode = configNode.Content[0].Content[i+1]
				break
			}
		}

		// Find and remove the source by name
		if destinationsNode != nil {
			for i := 0; i < len(destinationsNode.Content); i++ {
				destination := destinationsNode.Content[i]
				for j := 0; j < len(destination.Content); j += 2 {
					if destination.Content[j].Value == "name" && destination.Content[j+1].Value == destinationName {
						destinationsNode.Content = append(destinationsNode.Content[:i], destinationsNode.Content[i+1:]...)
						if err := utils.SaveConfigWithComments(configPath, configNode); err != nil {
							logger.Error().Str("err", err.Error()).Msg("error saving config")
							return
						}
						logger.Info().Msg("Destination removed successfully!")
						return
					}
				}
			}
			logger.Error().Msg("Destination not found.")
		} else {
			logger.Info().Msg("No destinations defined.")
		}
	},
}
