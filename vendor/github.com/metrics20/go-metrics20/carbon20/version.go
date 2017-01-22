package carbon20

import "strings"

type metricVersion int

const (
	Legacy      metricVersion = iota // bar.bytes or whatever
	M20                              // foo=bar.unit=B
	M20NoEquals                      // foo_is_bar.unit_is_B
)

// GetVersion returns the expected version of a metric, but doesn't validate
func GetVersion(metric_in string) metricVersion {
	if strings.Contains(metric_in, "=") {
		return M20
	}
	if strings.Contains(metric_in, "_is_") {
		return M20NoEquals
	}
	return Legacy
}

// GetVersionB is like getVersion but for byte array input.
func GetVersionB(metric_in []byte) metricVersion {
	for i, c := range metric_in {
		if c == 61 { // =
			return M20
		} else if c == 95 { // _ -> look for _is_
			if len(metric_in) > i+3 && metric_in[i+1] == 105 && metric_in[i+2] == 115 && metric_in[i+3] == 95 {
				return M20NoEquals
			}
		} else if c == 46 { // .
			return Legacy
		}
	}
	return Legacy
}

func IsMetric20(metric_in string) bool {
	v := GetVersion(metric_in)
	return v == M20 || v == M20NoEquals
}
