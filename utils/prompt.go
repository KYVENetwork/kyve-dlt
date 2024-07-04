package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
