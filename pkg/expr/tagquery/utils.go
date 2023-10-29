package tagquery

import (
	"strings"

	"github.com/grafana/metrictank/pkg/errors"
)

// validateQueryExpressionTagKey validates the key of a tag query expression
func validateQueryExpressionTagKey(key string) error {
	if len(key) == 0 {
		return errors.NewBadRequestf("Tag query expression key must not be empty")
	}

	if strings.ContainsAny(key, ";!^=") {
		return errors.NewBadRequestf("Invalid character in tag key %s ", key)
	}
	return nil
}
