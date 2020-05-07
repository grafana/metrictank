package cmd

import (
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/metricbuilder"
)

func getBuilder(metricBuilder, metricName string) metricbuilder.Builder {
	switch metricBuilder {
	case "tagged":
		return metricbuilder.Tagged{
			MetricName:          metricName,
			CustomTags:          customTags,
			AddTags:             addTags,
			NumUniqueCustomTags: numUniqueCustomTags,
			NumUniqueTags:       numUniqueTags,
		}
	case "simple":
		return metricbuilder.Simple{metricName}
	}
	panic("unknown metricbuilder :" + metricBuilder)
}
