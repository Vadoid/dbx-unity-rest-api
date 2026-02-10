package ui

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
)

func PrintInfo(msg string) {
	// Cyan for info
	c := color.New(color.FgCyan)
	c.Printf("ℹ️  %s\n", msg)
}

func PrintSuccess(msg string) {
	// Green for success
	c := color.New(color.FgGreen)
	c.Printf("✅ %s\n", msg)
}

func PrintError(msg string) {
	// Red for error
	c := color.New(color.FgRed)
	c.Printf("❌ %s\n", msg)
}

func PrintHeader(msg string) {
	// Bold Blue for major headers
	c := color.New(color.FgBlue, color.Bold)
	c.Printf("\n=== %s ===\n", msg)
}

func PrintTable(headers []string, rows [][]string) {
	if len(rows) == 0 {
		c := color.New(color.FgYellow)
		c.Printf("⚠️  No data found.\n")
		return
	}

	// Calculate column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Print Header
	headerColor := color.New(color.FgCyan, color.Bold)
	printRow(headers, widths, headerColor)

	// Print Separator
	sep := make([]string, len(headers))
	for i, w := range widths {
		sep[i] = strings.Repeat("-", w)
	}
	printRow(sep, widths, nil)

	// Print Rows
	for _, row := range rows {
		printRow(row, widths, nil)
	}
}

func PrintKeyValue(title string, data map[string]string) {
	PrintHeader(title)
	if len(data) == 0 {
		c := color.New(color.FgYellow)
		c.Printf("⚠️  No data available.\n")
		return
	}

	// Find max key length
	maxKeyLen := 0
	keys := make([]string, 0, len(data))
	for k := range data {
		if len(k) > maxKeyLen {
			maxKeyLen = len(k)
		}
		keys = append(keys, k)
	}

	// Sort keys? Maybe not necessary, but consistent.
	// Let's just print.
	keyColor := color.New(color.FgCyan, color.Bold)

	for _, k := range keys {
		pad := strings.Repeat(" ", maxKeyLen-len(k))
		keyColor.Printf("%s%s : ", k, pad)
		fmt.Printf("%s\n", data[k])
	}
}

func printRow(row []string, widths []int, c *color.Color) {
	var parts []string
	for i, cell := range row {
		// Simple padding
		pad := ""
		if i < len(widths) {
			pad = strings.Repeat(" ", widths[i]-len(cell))
		}
		parts = append(parts, cell+pad)
	}
	line := strings.Join(parts, "  ") // 2 spaces gap
	if c != nil {
		c.Println(line)
	} else {
		fmt.Println(line)
	}
}
