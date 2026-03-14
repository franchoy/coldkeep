package utils

import (
	"fmt"
	"time"
)

func PrintSuccess(title string) {
	fmt.Println(title)
}

func PrintDuration(start time.Time) {
	fmt.Printf("  Time: %v\n", time.Since(start))
}
