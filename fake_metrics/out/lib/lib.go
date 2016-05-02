package lib

import "github.com/raintank/raintank-metric/schema"

func Reslice(in []*schema.MetricData, size int) [][]*schema.MetricData {
	numSubSlices := len(in) / size
	if len(in)%size > 0 {
		numSubSlices += 1
	}
	out := make([][]*schema.MetricData, numSubSlices)
	for i := 0; i < numSubSlices; i++ {
		start := i * size
		end := (i + 1) * size
		if end > len(in) {
			out[i] = in[start:]
		} else {
			out[i] = in[start:end]
		}
	}
	return out
}
