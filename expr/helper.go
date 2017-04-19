package expr

import (
	"strings"

	"github.com/raintank/metrictank/api/models"
)

func patternsAsArgs(series []models.Series) string {
	var list []string
	set := make(map[string]struct{})
	for _, serie := range series {
		if _, ok := set[serie.QueryPatt]; ok {
			continue
		}
		list = append(list, serie.QueryPatt)
		set[serie.QueryPatt] = struct{}{}
	}
	return strings.Join(list, ",")
}
