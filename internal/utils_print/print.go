package utils_print

const MaxErrorsToPrint = 50

func AppendToErrorList(errorList []error, err error) []error {
	if len(errorList) < MaxErrorsToPrint {
		return append(errorList, err)
	}
	return errorList
}
