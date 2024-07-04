package utils

import (
	"bufio"
	"fmt"
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
		fmt.Print("\033[36mEnter number of prefered destination: \033[0m")
		input, _ := reader.ReadString('\n')
		choice, err := strconv.Atoi(strings.TrimSpace(input))
		if err == nil && choice > 0 && choice <= len(options) {
			return options[choice-1]
		}
		fmt.Println("Invalid choice, please try again.")
	}
}

func PromptInput(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
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
