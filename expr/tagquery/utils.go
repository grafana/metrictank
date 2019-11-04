package tagquery

import (
	"net/http"
	"strings"

	"github.com/grafana/metrictank/api/response"
)

// validateQueryExpressionTagKey validates the key of a tag query expression
func validateQueryExpressionTagKey(key string) error {
	if len(key) == 0 {
		return response.Errorf(http.StatusBadRequest, "Tag query expression key must not be empty")
	}

	if strings.ContainsAny(key, ";!^=") {
		return response.Errorf(http.StatusBadRequest, "Invalid character in tag key %s ", key)
	}
	return nil
}
