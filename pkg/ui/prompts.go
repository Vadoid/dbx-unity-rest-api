package ui

import (
	"strings"

	"github.com/manifoldco/promptui"
)

// SelectPrompt shows a list of items and returns the selected item and its index.
func SelectPrompt(label string, items []string) (int, string, error) {
	prompt := promptui.Select{
		Label: label,
		Items: items,
		Size:  10,
		Searcher: func(input string, index int) bool {
			// Simple robust search
			if index < 0 || index >= len(items) {
				return false
			}
			item := items[index]
			return strings.Contains(strings.ToLower(item), strings.ToLower(input))
		},
	}

	return prompt.Run()
}
