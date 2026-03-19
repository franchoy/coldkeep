package utils_print

import (
	"fmt"
	"time"
)

const MaxErrorsToPrint = 50

func PrintSuccess(title string) {
	fmt.Println(title)
}

func PrintDuration(start time.Time) {
	fmt.Printf("  Time: %v\n", time.Since(start))
}

func AppendToErrorList(errorList []error, err error) []error {
	if len(errorList) < MaxErrorsToPrint {
		return append(errorList, err)
	}
	return errorList
}
