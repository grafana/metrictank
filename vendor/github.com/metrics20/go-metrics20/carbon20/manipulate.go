// Package carbon20 provides functions that manipulate a metric string to represent a given operation
// if the metric is detected to be in metrics 2.0 format, the change
// will be in that style, if not, it will be a simple string prefix/postfix
// like legacy statsd.
package carbon20

import (
	"strings"
)

// DeriveCount represents a derive from counter to rate per second
func DeriveCount(in, p1, p2, p2ne string, m1Legacy bool) (out string) {
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		if m1Legacy {
			return p1 + in
		}
		out = p1 + in + ".rate"
	case M20:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = part + "ps"
			}
		}
		out = strings.Join(parts, ".")
		out = p2 + strings.Replace(out, "mtype=count", "mtype=rate", 1)
	case M20NoEquals:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = part + "ps"
			}
		}
		out = strings.Join(parts, ".")
		out = p2ne + strings.Replace(out, "mtype_is_count", "mtype_is_rate", 1)
	}
	return
}

// Gauge doesn't really represent a change in data format, so only apply the prefix.
func Gauge(in, p1, p2, p2ne string) (out string) {
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		out = p1 + in
	case M20:
		out = p2 + in
	case M20NoEquals:
		out = p2ne + in
	}
	return
}

// simpleStat is a helper function to help express some common statistical aggregations using the stat tag
// with an optional percentile or timespec specifier. underscores added automatically
func simpleStat(in, p1, p2, p2ne, stat1, stat2, percentile, timespec string) (out string) {
	if percentile != "" {
		percentile = "_" + percentile
	}
	if timespec != "" {
		timespec = "__" + timespec
	}
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		out = p1 + in + "." + stat1 + percentile + timespec
	case M20:
		out = p2 + in + ".stat=" + stat2 + percentile + timespec
	case M20NoEquals:
		out = p2ne + in + ".stat_is_" + stat2 + percentile + timespec
	}
	return
}

func Max(in, p1, p2, p2ne, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "upper", "max", percentile, timespec)
}

func Min(in, p1, p2, p2ne, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "lower", "min", percentile, timespec)
}

func Mean(in, p1, p2, p2ne, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "mean", "mean", percentile, timespec)
}

func Sum(in, p1, p2, p2ne, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "sum", "sum", percentile, timespec)
}

func Median(in, p1, p2, p2ne string, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "median", "median", percentile, timespec)
}

func Std(in, p1, p2, p2ne string, percentile, timespec string) (out string) {
	return simpleStat(in, p1, p2, p2ne, "std", "std", percentile, timespec)
}

// CountPckt reflects counting the amount of packets received for a given thing
func CountPckt(in, p1, p2, p2ne string) (out string) {
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		out = p1 + in + ".count"
	case M20:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pckt"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "mtype=") {
				parts[i] = "mtype=count"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		out = p2 + strings.Join(parts, ".")
	case M20NoEquals:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pckt"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "mtype_is_") {
				parts[i] = "mtype_is_count"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		out = p2ne + strings.Join(parts, ".")
	}
	return
}

// CountMetric reflects counting how many metrics were received
func CountMetric(in, p1, p2, p2ne string) (out string) {
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		out = p1 + in + ".count"
	case M20:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Metric"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "mtype=") {
				parts[i] = "mtype=count"
			}
		}
		out = p2 + strings.Join(parts, ".")
	case M20NoEquals:
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Metric"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "mtype_is_") {
				parts[i] = "mtype_is_count"
			}
		}
		out = p2ne + strings.Join(parts, ".")
	}
	return
}

// Count just reflects counting something each interval, keeping the unit
func Count(in, p1, p2, p2ne string, m1Legacy bool) (out string) {
	ver := GetVersion(in)
	switch ver {
	case Legacy:
		if m1Legacy {
			out = p1 + in
		} else {
			out = p1 + in + ".count"
		}
	case M20:
		parts := strings.Split(in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "mtype=") {
				ttSeen = true
				parts[i] = "mtype=count"
			}
		}
		if !ttSeen {
			parts = append(parts, "mtype=count")
		}
		out = p2 + strings.Join(parts, ".")
	case M20NoEquals:
		parts := strings.Split(in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "mtype_is_") {
				ttSeen = true
				parts[i] = "mtype_is_count"
			}
		}
		if !ttSeen {
			parts = append(parts, "mtype_is_count")
		}
		out = p2ne + strings.Join(parts, ".")
	}
	return
}

// Counter just reflects counting something across time, keeping the unit
func Counter(in, p1, p2, p2ne string) (out string) {
	ver := GetVersion(in)
	if ver == M20 {
		parts := strings.Split(in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "mtype=") {
				ttSeen = true
				parts[i] = "mtype=counter"
			}
		}
		if !ttSeen {
			parts = append(parts, "mtype=counter")
		}
		out = p2 + strings.Join(parts, ".")
	} else if ver == M20NoEquals {
		parts := strings.Split(in, ".")
		ttSeen := false
		for i, part := range parts {
			if strings.HasPrefix(part, "mtype_is_") {
				ttSeen = true
				parts[i] = "mtype_is_counter"
			}
		}
		if !ttSeen {
			parts = append(parts, "mtype_is_counter")
		}
		out = p2ne + strings.Join(parts, ".")
	} else {
		out = p1 + in + ".counter"
	}
	return
}

func RatePckt(in, p1, p2, p2ne string) (out string) {
	ver := GetVersion(in)
	if ver == M20 {
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pcktps"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "mtype=") {
				parts[i] = "mtype=rate"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		out = p2 + strings.Join(parts, ".")
	} else if ver == M20NoEquals {
		parts := strings.Split(in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pcktps"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "mtype_is_") {
				parts[i] = "mtype_is_rate"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		out = p2ne + strings.Join(parts, ".")
	} else {
		out = p1 + in + ".count_ps"
	}
	return
}
