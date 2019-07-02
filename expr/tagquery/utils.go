package tagquery

import (
	"fmt"
	"strings"
)

// validateQueryExpressionTagKey validates the key of a tag query expression
func validateQueryExpressionTagKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("Tag query expression key must not be empty")
	}

	if strings.ContainsAny(key, ";!^=") {
		return fmt.Errorf("Invalid character in tag key %s ", key)
	}
	return nil
}
