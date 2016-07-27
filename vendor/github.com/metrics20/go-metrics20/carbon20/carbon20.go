// Package carbon20 provides functions that manipulate a metric string to represent a given operation
// if the metric is detected to be in metrics 2.0 format, the change
// will be in that style, if not, it will be a simple string prefix/postfix
// like legacy statsd.
package carbon20

import (
	"strings"
)

// DeriveCount represents a derive from counter to rate per second
func DeriveCount(metric_in, m1Prefix string, m1Legacy bool) (metric_out string) {
	if IsMetric20(metric_in) {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") || strings.HasPrefix(part, "unit_is_") {
				parts[i] = part + "ps"
			}
		}
		metric_out = strings.Join(parts, ".")
		metric_out = strings.Replace(metric_out, "target_type=count", "target_type=rate", 1)
		metric_out = strings.Replace(metric_out, "target_type_is_count", "target_type_is_rate", 1)
		return
	}
	if m1Legacy {
		return m1Prefix + metric_in
	}
	return m1Prefix + metric_in + ".rate"
}

// Gauge doesn't really represent a change in data format, so for metrics 2.0 it doesn't change anything
func Gauge(metric_in, m1Prefix string) (metric_out string) {
	if IsMetric20(metric_in) {
		return metric_in
	}
	return m1Prefix + metric_in
}

// simpleStat is a helper function to help express some common statistical aggregations using the stat tag
// with an optional percentile or timespec specifier. underscores added automatically
func simpleStat(metric_in, m1Prefix, stat1, stat2, percentile, timespec string) (metric_out string) {
	if percentile != "" {
		percentile = "_" + percentile
	}
	if timespec != "" {
		timespec = "__" + timespec
	}
	v := GetVersion(metric_in)
	if v == M20 {
		return metric_in + ".stat=" + stat2 + percentile + timespec
	}
	if v == M20NoEquals {
		return metric_in + ".stat_is_" + stat2 + percentile + timespec
	}
	return m1Prefix + metric_in + "." + stat1 + percentile + timespec
}

func Max(metric_in, m1Prefix, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "upper", "max", percentile, timespec)
}

func Min(metric_in, m1Prefix, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "lower", "min", percentile, timespec)
}

func Mean(metric_in, m1Prefix, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "mean", "mean", percentile, timespec)
}

func Sum(metric_in, m1Prefix, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "sum", "sum", percentile, timespec)
}

func Median(metric_in, m1Prefix string, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "median", "median", percentile, timespec)
}

func Std(metric_in, m1Prefix string, percentile, timespec string) (metric_out string) {
	return simpleStat(metric_in, m1Prefix, "std", "std", percentile, timespec)
}

// CountPckt reflects counting the amount of packets received for a given thing
func CountPckt(metric_in, m1Prefix string) (metric_out string) {
	v := GetVersion(metric_in)
	if v == M20 {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pckt"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "target_type=") {
				parts[i] = "target_type=count"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else if v == M20NoEquals {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pckt"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "target_type_is_") {
				parts[i] = "target_type_is_count"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = m1Prefix + metric_in + ".count"
	}
	return
}

// CountMetric reflects counting how many metrics were received
func CountMetric(metric_in, m1Prefix string) (metric_out string) {
	v := GetVersion(metric_in)
	if v == M20 {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Metric"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "target_type=") {
				parts[i] = "target_type=count"
			}
		}
		metric_out = strings.Join(parts, ".")
	} else if v == M20NoEquals {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Metric"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "target_type_is_") {
				parts[i] = "target_type_is_count"
			}
		}
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = m1Prefix + metric_in + ".count"
	}
	return
}

// Count just reflects counting something each interval, keeping the unit
func Count(metric_in, m1Prefix string, m1Legacy bool) (metric_out string) {
	v := GetVersion(metric_in)
	if v == M20 {
		parts := strings.Split(metric_in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "target_type=") {
				ttSeen = true
				parts[i] = "target_type=count"
			}
		}
		if !ttSeen {
			parts = append(parts, "target_type=count")
		}
		metric_out = strings.Join(parts, ".")
	} else if v == M20NoEquals {
		parts := strings.Split(metric_in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "target_type_is_") {
				ttSeen = true
				parts[i] = "target_type_is_count"
			}
		}
		if !ttSeen {
			parts = append(parts, "target_type_is_count")
		}
		metric_out = strings.Join(parts, ".")
	} else if m1Legacy {
		metric_out = m1Prefix + metric_in
	} else {
		metric_out = m1Prefix + metric_in + ".count"
	}
	return
}

// Counter just reflects counting something across time, keeping the unit
func Counter(metric_in, m1Prefix string) (metric_out string) {
	v := GetVersion(metric_in)
	if v == M20 {
		parts := strings.Split(metric_in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "target_type=") {
				ttSeen = true
				parts[i] = "target_type=counter"
			}
		}
		if !ttSeen {
			parts = append(parts, "target_type=counter")
		}
		metric_out = strings.Join(parts, ".")
	} else if v == M20NoEquals {
		parts := strings.Split(metric_in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "target_type_is_") {
				ttSeen = true
				parts[i] = "target_type_is_counter"
			}
		}
		if !ttSeen {
			parts = append(parts, "target_type_is_counter")
		}
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = m1Prefix + metric_in + ".counter"
	}
	return
}

func RatePckt(metric_in, m1Prefix string) (metric_out string) {
	v := GetVersion(metric_in)
	if v == M20 {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pcktps"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "target_type=") {
				parts[i] = "target_type=rate"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else if v == M20NoEquals {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pcktps"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "target_type_is_") {
				parts[i] = "target_type_is_rate"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = m1Prefix + metric_in + ".count_ps"
	}
	return
}
