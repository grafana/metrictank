package tagQuery

import (
	"fmt"
	"strings"
)

func validateKey(key string) error {
	if strings.ContainsAny(key, ";!^") {
		return fmt.Errorf("Invalid character in tag key %s ", key)
	}
	return nil
}

func validateValue(value string) error {
	if strings.ContainsAny(value, ";~") {
		return fmt.Errorf("Invalid character in tag value %s", value)
	}
	return nil
}
