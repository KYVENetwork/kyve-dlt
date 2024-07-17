package utils

import (
	"bufio"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"strings"
)

func PromptConfirm(prompt string) bool {
	answer := ""
	fmt.Printf("\u001B[36m%s\u001B[0m", prompt)
	if _, err := fmt.Scan(&answer); err != nil {
		logger.Error().Str("err", err.Error()).Msg("failed to read user input")
		return false
	}
	return strings.ToLower(answer) == "y"
}

func PromptDestinationDropdown(prompt string, options []string) string {
	fmt.Println(prompt)
	for i, option := range options {
		fmt.Printf("%d: %s\n", i+1, option)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("\033[36mSelect destination type [1-%v]: \033[0m", len(options))
		input, _ := reader.ReadString('\n')
		choice, err := strconv.Atoi(strings.TrimSpace(input))
		if err == nil && choice > 0 && choice <= len(options) {
			return options[choice-1]
		}
		fmt.Println("Invalid choice, please try again.")
	}
}

func PromptInput(prompt string) string {
	var input string
	for {
		fmt.Print(prompt)
		reader := bufio.NewReader(os.Stdin)
		rawInput, _ := reader.ReadString('\n')
		input = strings.TrimSpace(rawInput)
		if input != "" {
			break
		}
		fmt.Println("Input cannot be empty. Please try again.")
	}
	return input
}

func PromptInputWithDefault(prompt string, defaultValue string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultValue
	}
	return input
}

func PromptPoolId(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		i, err := strconv.Atoi(input)
		if err == nil && i >= 0 {
			return input
		}
		fmt.Println("Invalid choice, please try again.")
	}
}

func PromptSchemaDropdown(prompt string, options []string) string {
	fmt.Println(prompt)
	for i, option := range options {
		fmt.Printf("%d: %s\n", i+1, option)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\033[36mEnter number of prefered schema: \033[0m")
		input, _ := reader.ReadString('\n')
		choice, err := strconv.Atoi(strings.TrimSpace(input))
		if err == nil && choice > 0 && choice <= len(options) {
			return options[choice-1]
		}
		fmt.Println("Invalid choice, please try again.")
	}
}

func PromptStepSize(prompt string, defaultValue string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			return defaultValue
		}
		i, err := strconv.Atoi(input)
		if err == nil && i > 0 {
			return input
		}
		fmt.Println("Invalid choice, please try again.")
	}
}

func SelectSource(configNode *yaml.Node) string {
	var sources []string
	for i, node := range configNode.Content[0].Content {
		if node.Value == "sources" {
			sourceNodes := configNode.Content[0].Content[i+1]
			for j := 0; j < len(sourceNodes.Content); j++ {
				sourceName := GetNodeValue(*sourceNodes.Content[j], "name")
				sources = append(sources, sourceName)
			}
			break
		}
	}

	if len(sources) == 0 {
		fmt.Println("No sources found in the configuration.")
		return ""
	}

	fmt.Println("\n\u001B[36mSelect a source from the existing sources: \u001B[0m")
	fmt.Println("0: custom source")
	for i, source := range sources {
		fmt.Printf("%d: %s\n", i+1, source)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\033[36mEnter number of preferred source: \033[0m")
		input, _ := reader.ReadString('\n')
		choice, err := strconv.Atoi(strings.TrimSpace(input))
		if err == nil && choice > 0 && choice <= len(sources) {
			return sources[choice-1]
		} else if choice == 0 && err == nil {
			return "custom"
		}
		fmt.Println("Invalid choice, please try again.")
	}
}
